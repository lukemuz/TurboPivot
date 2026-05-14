use crate::{PivotResult, RowKind};
use serde_json::Value;
use std::fmt::Write;

pub fn pivot_result_to_csv(result: &PivotResult) -> String {
    delimited(result, ',', true)
}

pub fn pivot_result_to_tsv(result: &PivotResult) -> String {
    delimited(result, '\t', false)
}

fn delimited(result: &PivotResult, sep: char, csv_quote: bool) -> String {
    let mut out = String::new();
    let escape = |s: &str| -> String {
        if !csv_quote {
            // TSV: strip tab + newline to keep one row per record.
            return s.replace('\t', " ").replace('\n', " ");
        }
        if s.contains(sep) || s.contains('"') || s.contains('\n') {
            format!("\"{}\"", s.replace('"', "\"\""))
        } else {
            s.to_string()
        }
    };

    let mut headers: Vec<String> = result.row_headers.clone();
    for ch in &result.column_headers {
        let label = if ch.column_values.is_empty() {
            ch.agg_label.clone()
        } else {
            format!("{} ({})", ch.agg_label, ch.column_values.join(" / "))
        };
        headers.push(label);
    }

    out.push_str(
        &headers
            .iter()
            .map(|h| escape(h))
            .collect::<Vec<_>>()
            .join(&sep.to_string()),
    );
    out.push('\n');

    for (i, row) in result.data.iter().enumerate() {
        let meta = result.row_meta.get(i);
        let label_prefix = match meta.map(|m| &m.kind) {
            Some(RowKind::Subtotal) => "Subtotal: ",
            _ => "",
        };

        let mut cells: Vec<String> = result
            .row_headers
            .iter()
            .map(|rh| {
                let raw = value_to_string(row.get(rh));
                if rh == &result.row_headers[0] {
                    format!("{}{}", label_prefix, raw)
                } else {
                    raw
                }
            })
            .collect();
        for ch in &result.column_headers {
            cells.push(value_to_string(row.get(&ch.key)));
        }
        out.push_str(
            &cells
                .iter()
                .map(|c| escape(c))
                .collect::<Vec<_>>()
                .join(&sep.to_string()),
        );
        out.push('\n');
    }

    if let Some(gt) = &result.grand_total {
        let mut cells: Vec<String> = result
            .row_headers
            .iter()
            .enumerate()
            .map(|(idx, _)| if idx == 0 { "Grand Total".to_string() } else { String::new() })
            .collect();
        for ch in &result.column_headers {
            cells.push(value_to_string(gt.get(&ch.key)));
        }
        out.push_str(
            &cells
                .iter()
                .map(|c| escape(c))
                .collect::<Vec<_>>()
                .join(&sep.to_string()),
        );
        out.push('\n');
    }

    out
}

fn value_to_string(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(f) = n.as_f64() {
                let mut s = String::new();
                let _ = write!(s, "{}", f);
                s
            } else {
                n.to_string()
            }
        }
        Some(Value::Bool(b)) => b.to_string(),
        Some(other) => other.to_string(),
    }
}
