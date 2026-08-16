//! Structured diagnostics for Mooncake business responses.

use crate::mooncake::ErrorCode;
use crate::segment::{ClientId, SegmentId};
use std::fmt;

const BUSINESS_LOG_TARGET: &str = "cakemaster::server::rpc::business";
const MAPPING_LOG_TARGET: &str = "cakemaster::server::rpc::mapping";

#[derive(Clone, Copy)]
pub(super) struct RpcLabels<'a> {
    operation: &'static str,
    client_id: Option<ClientId>,
    segment_id: Option<SegmentId>,
    tenant_id: Option<&'a str>,
}

impl<'a> RpcLabels<'a> {
    pub(super) const fn new(operation: &'static str) -> Self {
        Self {
            operation,
            client_id: None,
            segment_id: None,
            tenant_id: None,
        }
    }

    pub(super) const fn with_client(mut self, client_id: ClientId) -> Self {
        self.client_id = Some(client_id);
        self
    }

    pub(super) const fn with_segment(mut self, segment_id: SegmentId) -> Self {
        self.segment_id = Some(segment_id);
        self
    }

    pub(super) const fn with_tenant(mut self, tenant_id: &'a str) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }
}

pub(super) fn observe_result<T>(
    labels: RpcLabels<'_>,
    result: Result<T, ErrorCode>,
) -> Result<T, ErrorCode> {
    if let Err(error) = result.as_ref() {
        emit_failure(labels, 1, 1, *error);
    }
    result
}

pub(super) fn observe_batch<T>(
    labels: RpcLabels<'_>,
    results: Vec<Result<T, ErrorCode>>,
) -> Vec<Result<T, ErrorCode>> {
    let mut failed_items = 0;
    let mut representative = None;
    for error in results.iter().filter_map(|result| result.as_ref().err()) {
        failed_items += 1;
        if representative.is_none_or(|current| severity(*error) > severity(current)) {
            representative = Some(*error);
        }
    }
    if let Some(error) = representative {
        emit_failure(labels, results.len(), failed_items, error);
    }
    results
}

pub(super) fn observe_internal_mapping(
    component: &'static str,
    source_error: &impl fmt::Display,
    error_code: ErrorCode,
) {
    if error_code != ErrorCode::InternalError {
        return;
    }
    let context = coro_rpc::current_request_context();
    let request_sequence = DisplayOption(context.as_ref().map(|context| context.sequence));
    let function_id = DisplayOption(context.as_ref().map(|context| context.function_id));
    let peer_addr = DisplayOption(context.as_ref().map(|context| context.peer_addr));
    log::error!(
        target: MAPPING_LOG_TARGET,
        component = component,
        request_sequence:% = request_sequence,
        function_id:% = function_id,
        peer_addr:% = peer_addr,
        source_error:% = source_error;
        "domain error mapped to an internal RPC error"
    );
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Severity {
    Debug,
    Warn,
    Error,
}

fn severity(error: ErrorCode) -> Severity {
    if error == ErrorCode::InternalError {
        Severity::Error
    } else if matches!(
        error,
        ErrorCode::BufferOverflow
            | ErrorCode::NoAvailableHandle
            | ErrorCode::RpcFail
            | ErrorCode::RpcTimeout
            | ErrorCode::UnavailableInCurrentStatus
            | ErrorCode::TenantQuotaExceeded
    ) {
        Severity::Warn
    } else {
        Severity::Debug
    }
}

fn emit_failure(labels: RpcLabels<'_>, item_count: usize, failed_items: usize, error: ErrorCode) {
    let context = coro_rpc::current_request_context();
    let request_sequence = DisplayOption(context.as_ref().map(|context| context.sequence));
    let peer_addr = DisplayOption(context.as_ref().map(|context| context.peer_addr));
    let client_id = DisplayOption(labels.client_id);
    let segment_id = DisplayOption(labels.segment_id);
    let tenant_id = labels.tenant_id.unwrap_or("-");
    let error_code = i32::from(error);

    match severity(error) {
        Severity::Error => log::error!(
            target: BUSINESS_LOG_TARGET,
            operation = labels.operation,
            request_sequence:% = request_sequence,
            peer_addr:% = peer_addr,
            client_id:% = client_id,
            segment_id:% = segment_id,
            tenant_id = tenant_id,
            item_count = item_count,
            failed_items = failed_items,
            error_code = error_code,
            error_name:? = error;
            "Mooncake RPC business operation failed"
        ),
        Severity::Warn => log::warn!(
            target: BUSINESS_LOG_TARGET,
            operation = labels.operation,
            request_sequence:% = request_sequence,
            peer_addr:% = peer_addr,
            client_id:% = client_id,
            segment_id:% = segment_id,
            tenant_id = tenant_id,
            item_count = item_count,
            failed_items = failed_items,
            error_code = error_code,
            error_name:? = error;
            "Mooncake RPC business operation was rejected"
        ),
        Severity::Debug => log::debug!(
            target: BUSINESS_LOG_TARGET,
            operation = labels.operation,
            request_sequence:% = request_sequence,
            peer_addr:% = peer_addr,
            client_id:% = client_id,
            segment_id:% = segment_id,
            tenant_id = tenant_id,
            item_count = item_count,
            failed_items = failed_items,
            error_code = error_code,
            error_name:? = error;
            "Mooncake RPC business operation was rejected"
        ),
    }
}

struct DisplayOption<T>(Option<T>);

impl<T: fmt::Display> fmt::Display for DisplayOption<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(value) => value.fmt(formatter),
            None => formatter.write_str("-"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn business_error_severity_separates_faults_pressure_and_client_rejections() {
        assert_eq!(severity(ErrorCode::InternalError), Severity::Error);
        assert_eq!(severity(ErrorCode::NoAvailableHandle), Severity::Warn);
        assert_eq!(severity(ErrorCode::TenantQuotaExceeded), Severity::Warn);
        assert_eq!(severity(ErrorCode::InvalidParams), Severity::Debug);
        assert_eq!(severity(ErrorCode::ObjectNotFound), Severity::Debug);
    }

    #[test]
    fn display_option_uses_a_stable_missing_value() {
        assert_eq!(DisplayOption(Some(17)).to_string(), "17");
        assert_eq!(DisplayOption::<u32>(None).to_string(), "-");
    }
}
