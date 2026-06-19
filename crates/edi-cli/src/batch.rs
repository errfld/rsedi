use serde::Serialize;

use crate::BatchOutputFormat;

#[derive(Debug, Serialize)]
pub(crate) struct BatchSummary {
    pub(crate) command: &'static str,
    pub(crate) processed: usize,
    pub(crate) succeeded: usize,
    pub(crate) warned: usize,
    pub(crate) failed: usize,
    pub(crate) quarantined: usize,
    pub(crate) outputs: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct BatchFileOutcome {
    pub(crate) source: String,
    pub(crate) status: &'static str,
    pub(crate) messages: usize,
    pub(crate) errors: usize,
    pub(crate) warnings: usize,
    pub(crate) output: Option<String>,
    pub(crate) quarantine_id: Option<String>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BatchReport {
    pub(crate) summary: BatchSummary,
    pub(crate) files: Vec<BatchFileOutcome>,
}

pub(crate) fn build_batch_report(
    command: &'static str,
    files: Vec<BatchFileOutcome>,
    quarantined: usize,
) -> BatchReport {
    let summary = BatchSummary {
        command,
        processed: files.len(),
        succeeded: files.iter().filter(|file| file.status == "success").count(),
        warned: files.iter().filter(|file| file.status == "warning").count(),
        failed: files.iter().filter(|file| file.status == "failed").count(),
        quarantined,
        outputs: files.iter().filter(|file| file.output.is_some()).count(),
    };
    BatchReport { summary, files }
}

pub(crate) fn write_batch_report(
    report: &BatchReport,
    format: BatchOutputFormat,
) -> anyhow::Result<()> {
    match format {
        BatchOutputFormat::Json => {
            println!("{}", serde_json::to_string(report)?);
        }
        BatchOutputFormat::Text => {
            println!(
                "Batch {} summary: processed={}, succeeded={}, warnings={}, failed={}, quarantined={}, outputs={}",
                report.summary.command,
                report.summary.processed,
                report.summary.succeeded,
                report.summary.warned,
                report.summary.failed,
                report.summary.quarantined,
                report.summary.outputs
            );
            for file in &report.files {
                println!("{}: {}", file.status, file.source);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(status: &'static str, output: Option<&str>) -> BatchFileOutcome {
        BatchFileOutcome {
            source: format!("{status}.edi"),
            status,
            messages: 1,
            errors: usize::from(status == "failed"),
            warnings: usize::from(status == "warning"),
            output: output.map(str::to_owned),
            quarantine_id: None,
            error: None,
        }
    }

    #[test]
    fn batch_report_summary_counts_statuses_and_outputs() {
        let report = build_batch_report(
            "transform",
            vec![
                outcome("success", Some("success.json")),
                outcome("warning", Some("warning.json")),
                outcome("failed", None),
            ],
            1,
        );

        assert_eq!(report.summary.command, "transform");
        assert_eq!(report.summary.processed, 3);
        assert_eq!(report.summary.succeeded, 1);
        assert_eq!(report.summary.warned, 1);
        assert_eq!(report.summary.failed, 1);
        assert_eq!(report.summary.quarantined, 1);
        assert_eq!(report.summary.outputs, 2);
    }
}
