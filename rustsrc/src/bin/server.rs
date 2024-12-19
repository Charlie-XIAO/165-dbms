use std::io::Read;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::{fs, io};

use anyhow::{Context, Error};
use cs165::client_context::ClientContext;
use cs165::consts::SOCK_PATH;
use cs165::db::Db;
use cs165::message::{ClientMessage, ServerMessage};
use cs165::parse::parse;
use cs165::threads::{is_multi_threaded, set_multi_threaded};
use csv::ReaderBuilder;

fn process_command(
    query: String,
    db: &mut Db,
    cc: &mut ClientContext,
    stream: &mut UnixStream,
) -> Result<bool, Error> {
    if query == "shutdown" {
        bincode::serialize_into(stream, &ServerMessage::OkTerminate)
            .context("Failed to send message")?;
        return Ok(true);
    }

    let server_message = if query == "single_core()" {
        if !is_multi_threaded() {
            ServerMessage::ExecutionError("Already in single-core mode".to_string())
        } else {
            set_multi_threaded(false);
            ServerMessage::Ok
        }
    } else if query == "single_core_execute()" {
        if is_multi_threaded() {
            ServerMessage::ExecutionError("Not in single-core mode".to_string())
        } else {
            set_multi_threaded(true);
            ServerMessage::Ok
        }
    } else {
        match parse(&query) {
            (None, ServerMessage::Ok) => ServerMessage::Ok,
            (Some(command), ServerMessage::Ok) => command.execute(db, cc),
            (_, message) => message,
        }
    };

    bincode::serialize_into(stream, &server_message).context("Failed to send message")?;
    Ok(false)
}

fn process_csv(size: u64, db: &mut Db, stream: &UnixStream) -> Result<bool, Error> {
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(stream.take(size));

    let server_message = match db.load_csv(&mut csv_reader) {
        Ok(_) => ServerMessage::Ok,
        Err(e) => {
            let mut csv_stream = csv_reader.into_inner();
            let _ = io::copy(&mut csv_stream, &mut io::sink());
            ServerMessage::ExecutionError(e.to_string())
        },
    };

    bincode::serialize_into(stream, &server_message).context("Failed to send message")?;
    Ok(false)
}

fn handle_client(stream: UnixStream, db: &mut Db) -> Result<bool, Error> {
    let mut stream = stream;
    let mut cc = ClientContext::default();

    loop {
        let client_message: ClientMessage = match bincode::deserialize_from(&stream) {
            Ok(message) => message,
            Err(e) => match *e {
                bincode::ErrorKind::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    println!("Client disconnected");
                    break;
                },
                _ => return Err(e).context("Failed to receive message"),
            },
        };

        let shutdown_requested = match client_message {
            ClientMessage::RequestProcessCommand(query) => {
                process_command(query, db, &mut cc, &mut stream)?
            },
            ClientMessage::RequestProcessCSV(size) => process_csv(size, db, &stream)?,
        };

        if shutdown_requested {
            return Ok(true);
        }
    }

    Ok(false)
}

fn main() -> Result<(), Error> {
    let mut db = Db::launch().context("System failed to launch")?;

    if Path::new(SOCK_PATH).exists() {
        fs::remove_file(SOCK_PATH).context("Failed to unlink socket file")?;
    }
    let listener = UnixListener::bind(SOCK_PATH).context("Failed to bind to socket")?;
    println!("Waiting for client connections...");
    for stream in listener.incoming() {
        let stream = stream.context("Failed to accept connection")?;
        println!("Client connection established");
        if handle_client(stream, &mut db)? {
            break; // Shutdown requested
        }
    }

    db.shutdown().context("System failed to shutdown")?;
    Ok(())
}
