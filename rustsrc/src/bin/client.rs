use std::fs::File;
use std::io::{self, BufRead, Write};
use std::os::unix::net::UnixStream;

use anyhow::{Context, Error, Ok};
use cs165::consts::SOCK_PATH;
use cs165::message::{ClientMessage, ServerMessage};

fn send_command(command: &str, stream: &mut UnixStream) -> Result<(), Error> {
    let client_message = ClientMessage::RequestProcessCommand(command.to_string());
    bincode::serialize_into(stream, &client_message).context("Failed to send message")
}

fn send_csv(command: &str, stream: &mut UnixStream) -> Result<(), Error> {
    let path = command
        .trim_start_matches("load(")
        .trim_end_matches(")")
        .trim_matches('"');

    let mut file = File::open(path).context("Failed to open file")?;
    let metadata = file.metadata().context("Failed to read file metadata")?;
    let client_message = ClientMessage::RequestProcessCSV(metadata.len());
    bincode::serialize_into(&mut *stream, &client_message).context("Failed to send message")?;
    io::copy(&mut file, stream).context("Failed to send file")?;
    Ok(())
}

fn main() -> Result<(), Error> {
    let mut stream = UnixStream::connect(SOCK_PATH).context("Failed to connect to server")?;
    let is_atty = atty::is(atty::Stream::Stdin);

    let stdin = io::stdin();
    let mut handle = stdin.lock();
    let mut buffer = String::new();

    loop {
        if is_atty {
            print!("\x1b[1;32mclient>\x1b[0m ");
            io::stdout().flush().context("Failed to flush stdout")?;
        }

        buffer.clear();
        let bytes_read = handle
            .read_line(&mut buffer)
            .context("Failed to read from stdin")?;
        if bytes_read == 0 {
            break;
        }

        let command = buffer.trim();
        if command.is_empty() {
            continue;
        } else if command.starts_with("load(") {
            send_csv(command, &mut stream)?;
        } else {
            send_command(command, &mut stream)?;
        }

        let server_message: ServerMessage =
            bincode::deserialize_from(&mut stream).context("Failed to receive message")?;
        match server_message {
            ServerMessage::Ok => {},
            ServerMessage::OkTerminate => break,
            ServerMessage::OkWithPayload(payload) => println!("{}", payload),
            ServerMessage::InvalidCommand => eprintln!("\x1b[31mInvalid command\x1b[0m"),
            ServerMessage::ParseError(error) => eprintln!("\x1b[31mParse error: {}\x1b[0m", error),
            ServerMessage::BatchError(error) => eprintln!("\x1b[31mBatch error: {}\x1b[0m", error),
            ServerMessage::ExecutionError(error) => {
                eprintln!("\x1b[31mExecution error: {}\x1b[0m", error)
            },
            ServerMessage::UnknownExecutionError => {
                eprintln!("\x1b[31mUnknown execution error\x1b[0m")
            },
        }
    }

    Ok(())
}
