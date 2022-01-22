use clap::Parser;
use std::io::stdout;
use std::io::{self, BufRead, Write};

///ndb command
#[derive(Parser, Debug)]
#[clap(author,version,about, long_about= None)]
struct Args {
    ///server host
    #[clap(short, long, default_value = "localhost")]
    host: String,

    ///server port
    #[clap(short, long, default_value_t = 8888)]
    port: i16,
}

pub fn read_cmd() {
    let args = Args::parse();
    println!("args:{:?}", args);
    loop {
        print!("--> ");
        stdout().flush().unwrap();
        let mut buf = String::new();
        let stdin = io::stdin();
        let mut handle = stdin.lock();
        let result = handle.read_line(&mut buf);
        match result {
            Ok(input) => {
                println!("Input:{},Size:{}", &buf, input);
            }
            Err(error) => {
                println!("err in cmd, info:{:?}", error);
            }
        }
    }
}
