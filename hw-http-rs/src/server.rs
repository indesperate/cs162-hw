use std::env;
use std::net::{Ipv4Addr, SocketAddrV4};

use crate::args;

use crate::http::*;
use crate::stats::*;

use clap::Parser;
use std::path::Path;
use std::path::PathBuf;
use tokio::net::{TcpListener, TcpStream};

use anyhow::{Error, Result};

pub fn main() -> Result<()> {
    // Configure logging
    // You can print logs (to stderr) using
    // `log::info!`, `log::warn!`, `log::error!`, etc.
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    // Parse command line arguments
    let args = args::Args::parse();

    // Set the current working directory
    env::set_current_dir(&args.files)?;

    // Print some info for debugging
    log::info!("HTTP server initializing ---------");
    log::info!("Port:\t\t{}", args.port);
    log::info!("Num threads:\t{}", args.num_threads);
    log::info!("Directory:\t\t{}", &args.files);
    log::info!("----------------------------------");

    // Initialize a thread pool that starts running `listen`
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args.num_threads)
        .build()?
        .block_on(listen(args.port))
}

async fn listen(port: u16) -> Result<()> {
    let address = "0.0.0.0";
    let socket_address = format!("{}:{}", address, port);
    let listener = TcpListener::bind(&socket_address).await?;
    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move { handle_socket(socket).await });
    }
}

// Handles a single connection via `socket`.
async fn handle_socket(mut socket: TcpStream) -> Result<()> {
    let request = parse_request(&mut socket).await?;
    log::info!("method: {}, request: {}", &request.method, &request.path);
    match request.method.as_str() {
        "GET" => {
            handle_get_request(&mut socket, &request).await?;
        }
        _ => {
            send_404_default(&mut socket).await?;
        }
    }
    Ok(())
}

async fn handle_get_request(socket: &mut TcpStream, request: &Request) -> Result<()> {
    let path = format!(".{}", &request.path);
    let mut path_buf = PathBuf::from(path);
    if path_buf.exists() {
        if path_buf.is_dir() {
            path_buf.push("index.html");
            if path_buf.exists() {
                let path_str = path_buf.into_os_string().into_string().unwrap();
                send_file(socket, &path_str).await?;
            } else {
                let parent = path_buf.parent().unwrap();
                send_files_info(socket, parent).await?;
            }
        } else {
            let path_str = path_buf.into_os_string().into_string().unwrap();
            send_file(socket, &path_str).await?;
        }
    } else {
        send_404_default(socket).await?;
    }
    Ok(())
}

async fn send_404_default(socket: &mut TcpStream) -> Result<()> {
    start_response(socket, 404).await?;
    end_headers(socket).await?;
    Ok(())
}

async fn send_files_info(socket: &mut TcpStream, dir: &Path) -> Result<()> {
    start_response(socket, 200).await?;
    let content_type: &str = "text/html";
    let mut html_string = String::from("");
    let mut files = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = files.next_entry().await? {
        let file_str = entry.file_name().into_string().unwrap();
        let path_str = entry.path().into_os_string().into_string().unwrap();
        log::info!("{:?}", &path_str);
        let link_str = format_href(&path_str, &file_str);
        html_string.push_str(&link_str);
    }
    let content_length: String = html_string.len().to_string();
    write_content_info(socket, content_type, &content_length).await?;
    end_headers(socket).await?;
    write_str(socket, &html_string).await?;
    Ok(())
}

async fn send_file(socket: &mut TcpStream, path: &str) -> Result<()> {
    let mut file = open_file(path).await?;
    start_response(socket, 200).await?;
    let content_type: &str = get_mime_type(path);
    let content_length: String = file.metadata().await?.len().to_string();
    write_content_info(socket, content_type, &content_length).await?;
    end_headers(socket).await?;
    write_file(socket, &mut file).await?;
    Ok(())
}

async fn open_file(path: &str) -> Result<tokio::fs::File> {
    log::info!("open file {}", path);
    Ok(tokio::fs::OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(path)
        .await?)
}

// You are free (and encouraged) to add other funtions to this file.
// You can also create your own modules as you see fit.
