use pyo3::prelude::*;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::types::PyList;
use serde_json::json;

use crate::common::NonRetriableException;

/// Base class for headers strategies (subclassable from Python)
#[pyclass(subclass)]
#[derive(Clone)]
pub struct HeadersProvider {}

#[pymethods]
impl HeadersProvider {
    #[new]
    #[pyo3(signature = (**_kwargs))]
    fn new(_kwargs: Option<&pyo3::types::PyDict>) -> Self {
        // Accept and ignore kwargs to allow Python subclasses to pass their own arguments
        Self {}
    }

    /// Returns headers for gRPC metadata
    ///
    /// Subclasses must implement this method.
    ///
    /// Returns:
    ///     List of (header_name, header_value) tuples
    fn get_headers(&self, _py: Python) -> PyResult<PyObject> {
        Err(PyNotImplementedError::new_err(
            "Subclasses must implement get_headers()"
        ))
    }
}

/// OAuth 2.0 Client Credentials flow headers provider
#[pyclass(extends=HeadersProvider)]
pub struct OAuthHeadersProvider {
    workspace_id: String,
    workspace_url: String,
    table_name: String,
    client_id: String,
    client_secret: String,
}

#[pymethods]
impl OAuthHeadersProvider {
    #[new]
    fn new(
        workspace_id: String,
        workspace_url: String,
        table_name: String,
        client_id: String,
        client_secret: String,
    ) -> (Self, HeadersProvider) {
        (
            Self {
                workspace_id,
                workspace_url,
                table_name,
                client_id,
                client_secret,
            },
            HeadersProvider {},
        )
    }

    /// Fetch OAuth token and return authorization header
    fn get_headers(&self, py: Python) -> PyResult<PyObject> {
        let token = get_zerobus_token(
            &self.table_name,
            &self.workspace_id,
            &self.workspace_url,
            &self.client_id,
            &self.client_secret,
        )?;

        let headers = PyList::empty(py);
        headers.append(("authorization", format!("Bearer {}", token)))?;
        headers.append(("x-databricks-zerobus-table-name", self.table_name.clone()))?;

        Ok(headers.into())
    }
}

/// Fetches a Zerobus access token from Databricks OAuth endpoint
fn get_zerobus_token(
    table_name: &str,
    workspace_id: &str,
    workspace_url: &str,
    client_id: &str,
    client_secret: &str,
) -> PyResult<String> {
    // Parse table name
    let parts: Vec<&str> = table_name.split('.').collect();
    if parts.len() != 3 {
        return Err(NonRetriableException::new_err(format!(
            "Table name '{}' must be in the format of catalog.schema.table",
            table_name
        )));
    }

    let catalog_name = parts[0];
    let schema_name = parts[1];
    let table_name_part = parts[2];

    // Build authorization_details
    let authorization_details = json!([
        {
            "type": "unity_catalog_privileges",
            "privileges": ["USE CATALOG"],
            "object_type": "CATALOG",
            "object_full_path": catalog_name,
        },
        {
            "type": "unity_catalog_privileges",
            "privileges": ["USE SCHEMA"],
            "object_type": "SCHEMA",
            "object_full_path": format!("{}.{}", catalog_name, schema_name),
        },
        {
            "type": "unity_catalog_privileges",
            "privileges": ["SELECT", "MODIFY"],
            "object_type": "TABLE",
            "object_full_path": format!("{}.{}.{}", catalog_name, schema_name, table_name_part),
        },
    ]);

    let url = format!("{}/oidc/v1/token", workspace_url);

    // Build request body
    let body = [
        ("grant_type", "client_credentials"),
        ("scope", "all-apis"),
        (
            "resource",
            &format!("api://databricks/workspaces/{}/zerobusDirectWriteApi", workspace_id),
        ),
        ("authorization_details", &authorization_details.to_string()),
    ];

    // Make HTTP request using reqwest synchronously
    // Note: In production, we should use tokio runtime for async
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| NonRetriableException::new_err(format!("Failed to create runtime: {}", e)))?;

    let result = runtime.block_on(async {
        let client = reqwest::Client::new();
        let response = client
            .post(&url)
            .basic_auth(client_id, Some(client_secret))
            .form(&body)
            .send()
            .await
            .map_err(|e| NonRetriableException::new_err(format!("Error making OAuth request: {}", e)))?;

        if !response.status().is_success() {
            return Err(NonRetriableException::new_err(format!(
                "OAuth request failed with status {}: {}",
                response.status(),
                response.text().await.unwrap_or_else(|_| "Unable to read error message".to_string())
            )));
        }

        let token_data: serde_json::Value = response
            .json()
            .await
            .map_err(|e| NonRetriableException::new_err(format!("Error parsing OAuth response: {}", e)))?;

        let access_token = token_data
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| NonRetriableException::new_err("No access token received from OAuth response"))?;

        Ok(access_token.to_string())
    });

    result
}
