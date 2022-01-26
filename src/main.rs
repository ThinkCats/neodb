use ndb::cli;
use ndb::store;

fn main() {
    println!("Welcome !");
    //init db
    store::startup_load_schema_mem();
    //read cmd
    cli::read_cmd();
}
