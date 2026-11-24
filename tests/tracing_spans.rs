//! End-to-end check that the index scan and record fetch phases emit `tracing` spans.
//!
//! Based on [`common::employee_provider`]

mod common;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

/// A span name with its fields, as set at creation or recorded later.
#[derive(Debug)]
struct CapturedSpan {
    name: &'static str,
    fields: HashMap<&'static str, String>,
    /// Number of times the span was entered.
    enters: usize,
}

/// Layer capturing every span. The index of a span in `spans` is stored in its extensions.
#[derive(Clone, Default)]
struct SpanCapture {
    spans: Arc<Mutex<Vec<CapturedSpan>>>,
}

struct FieldVisitor<'a>(&'a mut HashMap<&'static str, String>);

impl Visit for FieldVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for SpanCapture
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        attrs.record(&mut FieldVisitor(&mut fields));
        let mut spans = self.spans.lock().unwrap();
        spans.push(CapturedSpan {
            name: attrs.metadata().name(),
            fields,
            enters: 0,
        });
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(spans.len() - 1);
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let Some(&idx) = span.extensions().get::<usize>() else {
            return;
        };
        self.spans.lock().unwrap()[idx].enters += 1;
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let Some(&idx) = span.extensions().get::<usize>() else {
            return;
        };
        values.record(&mut FieldVisitor(
            &mut self.spans.lock().unwrap()[idx].fields,
        ));
    }
}

#[tokio::test]
async fn index_scan_and_record_fetch_emit_spans() -> Result<()> {
    let capture = SpanCapture::default();
    // Global so spans created on any runtime thread are captured. Only test in this binary.
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(capture.clone()))
        .expect("global subscriber already set");

    // `age > 30` matches {3, 5} through the age index, returned as one batch.
    let ctx = common::setup_test_env();
    let batches = ctx
        .sql("SELECT * FROM employees WHERE age > 30")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

    let captured = capture.spans.lock().unwrap();
    let spans_named = |name: &str| {
        captured
            .iter()
            .filter(|s| s.name == name)
            .collect::<Vec<_>>()
    };

    let scans = spans_named("index_scan");
    assert_eq!(scans.len(), 1, "index_scan spans: {scans:?}");
    assert_eq!(scans[0].fields["index"], "age_index");
    assert_eq!(scans[0].fields["table"], "employee");
    assert_eq!(scans[0].fields["partition"], "0");
    assert_eq!(scans[0].fields["filters"], "1");
    // Entered around `Index::scan`, then on each poll of the index stream (batch, end).
    assert!(
        scans[0].enters >= 3,
        "index_scan entered {} times",
        scans[0].enters
    );

    let fetches = spans_named("record_fetch");
    assert_eq!(fetches.len(), 1, "record_fetch spans: {fetches:?}");
    assert_eq!(fetches[0].fields["keys"], "2");
    assert_eq!(fetches[0].fields["fetched_rows"], "2");
    Ok(())
}
