use std::{
    env,
    path::Path,
};

use relational_db::{DB, Tuple, errors};
use rustyline::{DefaultEditor, error::ReadlineError};

fn print_table_row(row: &[String], max_lengths: &[usize]) {
    let mut line = String::new();
    for (i, col) in row.iter().enumerate() {
        line.push('|');
        line.push_str(&format!(" {:1$} ", col, max_lengths[i]));
    }
    line.push('|');
    println!("{}", line);
}

fn print_table(header: Vec<String>, iterator: impl Iterator<Item = errors::Result<Tuple>>) {
    let rows = iterator
        .map(|t| t.unwrap().into_values())
        .collect::<Vec<_>>();
    let string_rows = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut max_lengths: Vec<usize> = header.iter().map(|s| s.len()).collect();
    for row in &string_rows {
        for (i, value) in row.iter().enumerate() {
            max_lengths[i] = max_lengths[i].max(value.len());
        }
    }

    let separator = "-".repeat(max_lengths.iter().sum::<usize>() + header.len() * 3 + 1);

    print_table_row(&header, &max_lengths);
    println!("{}", separator);
    for row in string_rows {
        print_table_row(&row, &max_lengths);
    }
    println!("{}", separator);
}

fn main() {
    let storage_dir = Path::new(&env::var("STORAGE_DIR").unwrap_or("data".to_string())).to_owned();
    let mut db = if storage_dir.exists() {
        DB::load(storage_dir.clone())
    } else {
        DB::init(storage_dir.clone())
    }
    .expect("Failed to initialize or load the database");

    let mut rl = DefaultEditor::new().unwrap();
    let hist_file = storage_dir.join("history.txt");
    let _ = rl.load_history(&hist_file);

    println!("Use \\q to exit");
    println!("Use lowercase for SQL commands");
    println!("Don't use ;");
    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str()).unwrap();
                rl.save_history(&hist_file).unwrap();
                if line.is_empty() {
                    continue;
                }
                if line == "\\q" {
                    break;
                }
                match db.execute(&line) {
                    Ok((Some((header, iterator)), None)) => {
                        print_table(header, iterator);
                    }
                    Ok((None, Some(affected))) => {
                        println!("{} rows affected", affected);
                    }
                    Ok((None, None)) => {
                        println!("Query executed successfully");
                    }
                    Ok((Some(_), Some(_))) => {
                        println!("Error: meow");
                    }
                    Err(e) => {
                        println!("{}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
}
