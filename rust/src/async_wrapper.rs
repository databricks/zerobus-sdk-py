use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use pyo3_asyncio::tokio::future_into_py;
use tokio::sync::RwLock;

use databricks_zerobus_ingest_sdk::{
    databricks::zerobus::RecordType as RustRecordType, EncodedRecord,
    HeadersProvider as RustHeadersProvider, StreamConfigurationOptions as RustStreamOptions,
    TableProperties as RustTableProperties, ZerobusError as RustError, ZerobusResult as RustResult,
    ZerobusSdk as RustSdk, ZerobusStream as RustStream,
};

use crate::common::{map_error, StreamConfigurationOptions, TableProperties};

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

fn extract_record_payload(payload: &PyAny) -> PyResult<EncodedRecord> {
    if let Ok(bytes) = payload.downcast::<PyBytes>() {
        Ok(EncodedRecord::Proto(bytes.as_bytes().to_vec()))
    } else if let Ok(json_str) = payload.extract::<String>() {
        Ok(EncodedRecord::Json(json_str))
    } else if let Ok(bytes) = payload.extract::<Vec<u8>>() {
        Ok(EncodedRecord::Proto(bytes))
    } else if payload.hasattr("SerializeToString")? {
        // It's a protobuf Message object - serialize it
        let serialize_method = payload.getattr("SerializeToString")?;
        let serialized_bytes: Vec<u8> = serialize_method.call0()?.extract()?;
        Ok(EncodedRecord::Proto(serialized_bytes))
    } else {
        // Try to serialize as JSON (dict, list, etc.)
        Python::with_gil(|py| {
            let json_module = py.import("json")?;
            let json_dumps = json_module.getattr("dumps")?;
            let json_str: String = json_dumps.call1((payload,))?.extract()?;
            Ok(EncodedRecord::Json(json_str))
        })
    }
}

fn extract_record_payloads(payloads: &PyAny) -> PyResult<Vec<EncodedRecord>> {
    let mut record_payloads = Vec::new();

    if let Ok(list) = payloads.downcast::<PyList>() {
        record_payloads.reserve(list.len());

        for item in list {
            record_payloads.push(extract_record_payload(item)?);
        }
    } else if let Ok(bytes_list) = payloads.extract::<Vec<Vec<u8>>>() {
        for bytes in bytes_list {
            record_payloads.push(EncodedRecord::Proto(bytes));
        }
    } else if let Ok(json_list) = payloads.extract::<Vec<String>>() {
        for json in json_list {
            record_payloads.push(EncodedRecord::Json(json));
        }
    } else {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Payloads must be a list",
        ));
    }

    Ok(record_payloads)
}

fn map_rust_error_to_pyerr(err: RustError) -> PyErr {
    map_error(err)
}

// =============================================================================
// HEADERS PROVIDER WRAPPER
// =============================================================================

pub struct HeadersProviderWrapper {
    py_obj: PyObject,
}

impl HeadersProviderWrapper {
    pub fn new(py_obj: PyObject) -> Self {
        Self { py_obj }
    }
}

#[async_trait]
impl RustHeadersProvider for HeadersProviderWrapper {
    async fn get_headers<'a>(&'a self) -> RustResult<HashMap<&'static str, String>> {
        Python::with_gil(|py| {
            let py_headers = self.py_obj.call_method0(py, "get_headers")?;
            let list: &PyList = py_headers.extract(py)?;
            let mut headers = HashMap::new();
            for item in list.iter() {
                let tuple: &pyo3::types::PyTuple = item.extract()?;
                let key: String = tuple.get_item(0)?.extract()?;
                let value: String = tuple.get_item(1)?.extract()?;
                let key_static: &'static str = Box::leak(key.into_boxed_str());
                headers.insert(key_static, value);
            }
            Ok(headers)
        })
        .map_err(|e: PyErr| RustError::InvalidArgument(format!("Python headers error: {:?}", e)))
    }
}

// =============================================================================
// ACK FUTURE FOR LEGACY API
// =============================================================================

type AckFutureInner = Pin<Box<dyn Future<Output = PyResult<i64>> + Send + 'static>>;

/// A future that resolves with the acknowledgment ID of an ingested record.
/// This future can only be awaited once.
#[pyclass(name = "RecordIngestionFuture")]
pub struct PyAckFuture {
    inner: Arc<Mutex<Option<AckFutureInner>>>,
}

impl PyAckFuture {
    pub fn new(future: impl Future<Output = PyResult<i64>> + Send + 'static) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(Box::pin(future)))),
        }
    }
}

#[pymethods]
impl PyAckFuture {
    fn __await__<'py>(slf: PyRef<'_, Self>, py: Python<'py>) -> PyResult<&'py PyAny> {
        let inner_clone = slf.inner.clone();

        let rust_future = async move {
            let future_opt = {
                let mut guard = inner_clone.lock().unwrap();
                guard.take()
            };
            if let Some(future) = future_opt {
                future.await
            } else {
                Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "RecordIngestionFuture has already been awaited.",
                ))
            }
        };
        future_into_py(py, rust_future)
    }

    fn __repr__(&self) -> &'static str {
        "<RecordIngestionFuture (pending)>"
    }
}

// =============================================================================
// ZEROBUS STREAM (ASYNC)
// =============================================================================

#[pyclass]
pub struct ZerobusStream {
    inner: Arc<RwLock<RustStream>>,
}

#[pymethods]
impl ZerobusStream {
    /// Ingest a record and return a future that can be awaited for acknowledgment (legacy API)
    ///
    /// Args:
    ///     payload: Bytes (serialized protobuf) or str (JSON)
    ///
    /// Returns:
    ///     RecordIngestionFuture that can be awaited for acknowledgment
    #[deprecated(since = "0.3.0", note = "Use ingest_record_offset() instead for better performance")]
    fn ingest_record<'py>(&self, py: Python<'py>, payload: &PyAny) -> PyResult<&'py PyAny> {
        let record_payload = extract_record_payload(payload)?;
        let stream_clone = self.inner.clone();

        let outer_future = async move {
            let stream_guard = stream_clone.read().await;

            let ack_future_result = stream_guard.ingest_record(record_payload).await;
            drop(stream_guard);

            ack_future_result
                .map(|ack_future| {
                    let converted_future = async move {
                        ack_future.await.map_err(|e| {
                            Python::with_gil(|_py| map_rust_error_to_pyerr(e))
                        })
                    };
                    PyAckFuture::new(converted_future)
                })
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
        };

        future_into_py(py, outer_future)
    }

    /// Ingest a single record and return the offset ID (async)
    fn ingest_record_offset<'py>(&self, py: Python<'py>, payload: &PyAny) -> PyResult<&'py PyAny> {
        let record_payload = extract_record_payload(payload)?;
        let stream_clone = self.inner.clone();

        future_into_py(py, async move {
            let stream_guard = stream_clone.read().await;
            let offset_id = stream_guard
                .ingest_record_offset(record_payload)
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(offset_id)
        })
    }

    /// Ingest a single record without waiting (fire-and-forget async)
    fn ingest_record_nowait(&self, payload: &PyAny) -> PyResult<()> {
        let record_payload = extract_record_payload(payload)?;
        let stream_clone = self.inner.clone();

        // Spawn a background task
        pyo3_asyncio::tokio::get_runtime().spawn(async move {
            let stream_guard = stream_clone.read().await;
            let _ = stream_guard.ingest_record_offset(record_payload).await;
        });

        Ok(())
    }

    /// Ingest a batch of records and return one offset for the whole batch (async)
    fn ingest_records_offset<'py>(
        &self,
        py: Python<'py>,
        payloads: &PyAny,
    ) -> PyResult<&'py PyAny> {
        let record_payloads = extract_record_payloads(payloads)?;
        let stream_clone = self.inner.clone();

        future_into_py(py, async move {
            let stream_guard = stream_clone.read().await;
            let offset_id = stream_guard
                .ingest_records_offset(record_payloads)
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(offset_id)
        })
    }

    /// Ingest a batch of records without waiting (async)
    fn ingest_records_nowait(&self, payloads: &PyAny) -> PyResult<()> {
        let record_payloads = extract_record_payloads(payloads)?;
        let stream_clone = self.inner.clone();

        pyo3_asyncio::tokio::get_runtime().spawn(async move {
            let stream_guard = stream_clone.read().await;
            let _ = stream_guard.ingest_records_offset(record_payloads).await;
        });

        Ok(())
    }

    /// Wait for a specific offset to be acknowledged (async)
    fn wait_for_offset<'py>(&self, py: Python<'py>, offset: i64) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();

        future_into_py(py, async move {
            let stream_guard = stream.read().await;
            stream_guard
                .wait_for_offset(offset)
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(())
        })
    }

    /// Flush the stream (async)
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();

        future_into_py(py, async move {
            let stream_guard = stream.read().await;
            stream_guard
                .flush()
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(())
        })
    }

    /// Close the stream (async)
    fn close<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();

        future_into_py(py, async move {
            let mut stream_guard = stream.write().await;
            stream_guard
                .close()
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(())
        })
    }

    /// Get unacked records (placeholder for compatibility)
    fn get_unacked_records<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        future_into_py(py, async move { Ok::<Vec<PyObject>, PyErr>(vec![]) })
    }
}

// =============================================================================
// ZEROBUS SDK (ASYNC)
// =============================================================================

#[pyclass]
pub struct ZerobusSdk {
    inner: Arc<RwLock<RustSdk>>,
}

#[pymethods]
impl ZerobusSdk {
    #[new]
    fn new(host: String, unity_catalog_url: String) -> PyResult<Self> {
        let sdk = RustSdk::new(host, unity_catalog_url)
            .map_err(|err| Python::with_gil(|_py| map_rust_error_to_pyerr(err)))?;

        Ok(ZerobusSdk {
            inner: Arc::new(RwLock::new(sdk)),
        })
    }

    /// Set whether to use TLS (default: true)
    /// Set to false for testing with local mock servers
    fn set_use_tls<'py>(&self, py: Python<'py>, use_tls: bool) -> PyResult<&'py PyAny> {
        let sdk = self.inner.clone();

        future_into_py(py, async move {
            let mut sdk_guard = sdk.write().await;
            sdk_guard.use_tls = use_tls;
            Ok(())
        })
    }

    /// Create stream with client credentials (async)
    #[pyo3(signature = (client_id, client_secret, table_properties, options = None))]
    fn create_stream<'py>(
        &self,
        py: Python<'py>,
        client_id: String,
        client_secret: String,
        table_properties: &TableProperties,
        options: Option<&StreamConfigurationOptions>,
    ) -> PyResult<&'py PyAny> {
        let sdk = self.inner.clone();

        // Convert Python TableProperties to Rust TableProperties
        let props = RustTableProperties {
            table_name: table_properties.table_name.clone(),
            descriptor_proto: table_properties.descriptor_proto.clone(),
        };

        let opts = convert_stream_options(options);

        future_into_py(py, async move {
            let sdk_guard = sdk.read().await;
            sdk_guard
                .create_stream(props, client_id, client_secret, opts)
                .await
                .map(|stream| ZerobusStream {
                    inner: Arc::new(RwLock::new(stream)),
                })
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
        })
    }

    /// Create stream with custom headers provider (async)
    #[pyo3(signature = (table_properties, headers_provider, options = None))]
    fn create_stream_with_headers_provider<'py>(
        &self,
        py: Python<'py>,
        table_properties: &TableProperties,
        headers_provider: PyObject,
        options: Option<&StreamConfigurationOptions>,
    ) -> PyResult<&'py PyAny> {
        let sdk = self.inner.clone();

        let props = RustTableProperties {
            table_name: table_properties.table_name.clone(),
            descriptor_proto: table_properties.descriptor_proto.clone(),
        };

        let opts = convert_stream_options(options);
        let wrapper = Arc::new(HeadersProviderWrapper::new(headers_provider));

        future_into_py(py, async move {
            let sdk_guard = sdk.read().await;
            sdk_guard
                .create_stream_with_headers_provider(props, wrapper, opts)
                .await
                .map(|stream| ZerobusStream {
                    inner: Arc::new(RwLock::new(stream)),
                })
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
        })
    }

    /// Recreate a stream from an old stream (async)
    fn recreate_stream<'py>(
        &self,
        py: Python<'py>,
        old_stream: &ZerobusStream,
    ) -> PyResult<&'py PyAny> {
        let sdk = self.inner.clone();
        let old_stream_inner = old_stream.inner.clone();

        future_into_py(py, async move {
            let guard = old_stream_inner.read().await;
            let sdk_guard = sdk.read().await;
            let new_stream = sdk_guard
                .recreate_stream(&*guard)
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;

            Ok(ZerobusStream {
                inner: Arc::new(RwLock::new(new_stream)),
            })
        })
    }
}

// Helper to convert Python StreamConfigurationOptions to Rust options
fn convert_stream_options(
    options: Option<&StreamConfigurationOptions>,
) -> Option<RustStreamOptions> {
    options.map(|opts| {
        let mut rust_opts = RustStreamOptions::default();

        rust_opts.max_inflight_requests = opts.max_inflight_records as usize;
        rust_opts.recovery = opts.recovery;
        rust_opts.recovery_timeout_ms = opts.recovery_timeout_ms as u64;
        rust_opts.recovery_backoff_ms = opts.recovery_backoff_ms as u64;
        rust_opts.recovery_retries = opts.recovery_retries as u32;
        rust_opts.server_lack_of_ack_timeout_ms = opts.server_lack_of_ack_timeout_ms as u64;
        rust_opts.flush_timeout_ms = opts.flush_timeout_ms as u64;

        // Convert RecordType
        rust_opts.record_type = match opts.record_type.value {
            1 => RustRecordType::Proto,
            2 => RustRecordType::Json,
            _ => RustRecordType::Proto,
        };

        rust_opts
    })
}
