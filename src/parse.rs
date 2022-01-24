use std::fmt::Display;

use sqlparser::ast::{DataType, Expr, Query, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

pub trait DDL: Display {
    fn cmd(self: &Self) -> DbCmd;
}

#[derive(Debug)]
pub enum DbCmd {
    CreateDatabase,
    CreateTable,
    Select,
}

#[derive(Debug)]
struct SelectDef {
    columns: Vec<String>,
    table: Vec<String>,
}

#[derive(Debug)]
pub struct CreateTableDef {
    pub db_name: String,
    pub table_name: String,
    pub columns: Vec<ColDef>,
}

#[derive(Debug)]
pub struct ColDef {
    pub name: String,
    pub col_type: DataType,
}

impl DDL for CreateTableDef {
    fn cmd(self: &Self) -> DbCmd {
        DbCmd::CreateTable
    }
}
impl Display for CreateTableDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Table Name:{},Columns Def:{:?}",
            self.table_name, self.columns
        )
    }
}

impl DDL for SelectDef {
    fn cmd(self: &Self) -> DbCmd {
        DbCmd::Select
    }
}
impl Display for SelectDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Table Name:{:?},Columns:{:?}", self.table, self.columns)
    }
}

#[derive(Debug)]
struct CreateDbDef {
    db_name: String,
    if_not_exists: bool,
}

impl DDL for CreateDbDef {
    fn cmd(self: &Self) -> DbCmd {
        DbCmd::CreateDatabase
    }
}

impl Display for CreateDbDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Db Name:{}, If Not Existed:{}",
            self.db_name, self.if_not_exists
        )
    }
}

pub fn parse_sql(sql: &str) -> Box<dyn DDL> {
    let dialect = MySqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql);

    match ast {
        Ok(mut data) => {
            if data.len() > 1 {
                panic!("too much sql");
            }
            let statement = data.pop().unwrap();

            let ddl: Box<dyn DDL> = match statement {
                //process select query
                Statement::Query(q) => {
                    let select_def = parse_select_cols(q);
                    Box::new(select_def)
                }
                //process create db
                Statement::CreateSchema {
                    schema_name,
                    if_not_exists,
                } => {
                    let mut n = schema_name.0;
                    let name = n.pop().unwrap().value;
                    let create_db_def = CreateDbDef {
                        db_name: name,
                        if_not_exists,
                    };
                    Box::new(create_db_def)
                }
                //process create table
                Statement::CreateTable {
                    name,
                    columns,
                    or_replace: _,
                    temporary: _,
                    constraints: _,
                    hive_distribution: _,
                    hive_formats: _,
                    with_options: _,
                    file_format: _,
                    table_properties: _,
                    location: _,
                    query: _,
                    without_rowid: _,
                    like: _,
                    external: _,
                    if_not_exists: _,
                } => {
                    let mut name_vec = name.0;
                    let table_name = name_vec.pop().unwrap().value;

                    let mut col_defs = Vec::new();
                    for v in columns {
                        let col_name = v.name.value;
                        let data_type = v.data_type;
                        col_defs.push(ColDef {
                            name: col_name,
                            col_type: data_type,
                        });
                    }

                    let create_table_def = CreateTableDef {
                        table_name,
                        columns: col_defs,
                        //TODO db name in context
                        db_name: String::from("test"),
                    };
                    Box::new(create_table_def)
                }
                Statement::Insert {
                    or: _,
                    table_name,
                    columns,
                    overwrite: _,
                    source: _,
                    partitioned: _,
                    after_columns: _,
                    table,
                    on: _,
                } => {
                    println!("table_name:{}", table_name);
                    panic!()
                }
                _ => {
                    panic!("body parse error")
                }
            };

            ddl
        }
        Err(err) => {
            println!("find error:{:?}", err);
            panic!("parse error")
        }
    }
}

///parse select sql
fn parse_select_cols(q: Box<Query>) -> SelectDef {
    let mut cols: Vec<String> = Vec::new();
    let mut tables: Vec<String> = Vec::new();

    match q.body {
        SetExpr::Select(s) => {
            //columns parse
            for proj in s.projection {
                match proj {
                    SelectItem::UnnamedExpr(expr) => match expr {
                        Expr::Identifier(ident) => {
                            let value = ident.value;
                            cols.push(value);
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
            //table name parse,TODO join,alias
            for from in s.from {
                let relation = from.relation;
                match relation {
                    TableFactor::Table {
                        name,
                        alias: _,
                        args: _,
                        with_hints: _,
                    } => {
                        let names = name.0;
                        for ident in names {
                            tables.push(ident.value);
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    };

    SelectDef {
        columns: cols,
        table: tables,
    }
}
