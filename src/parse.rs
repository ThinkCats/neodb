use std::any::Any;
use std::fmt::Display;

use anyhow::{bail, Result};
use sqlparser::ast::{DataType, Expr, Query, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

///Database Operate
pub trait DOP: Display {
    fn cmd(self: &Self) -> DbCmd;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug)]
pub enum DbCmd {
    Show,
    Use,
    CreateDatabase,
    CreateTable,
    Select,
    Insert,
}

#[derive(Debug)]
pub struct SelectDef {
    pub columns: Vec<String>,
    pub table: Vec<String>,
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

#[derive(Debug)]
pub struct ShowDef {
    pub show_type: ShowType,
}

#[derive(Debug)]
pub enum ShowType {
    DataBase,
    Table,
}

impl DOP for ShowDef {
    fn cmd(self: &Self) -> DbCmd {
        DbCmd::Show
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for ShowDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Show Def Type:{:?}", self.show_type)
    }
}

impl DOP for CreateTableDef {
    fn cmd(self: &Self) -> DbCmd {
        DbCmd::CreateTable
    }

    fn as_any(&self) -> &dyn Any {
        self
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

impl DOP for SelectDef {
    fn cmd(self: &Self) -> DbCmd {
        DbCmd::Select
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
impl Display for SelectDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Table Name:{:?},Columns:{:?}", self.table, self.columns)
    }
}

#[derive(Debug)]
pub struct CreateDbDef {
    pub db_name: String,
    pub if_not_exists: bool,
}

impl DOP for CreateDbDef {
    fn cmd(self: &Self) -> DbCmd {
        DbCmd::CreateDatabase
    }

    fn as_any(&self) -> &dyn Any {
        self
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

#[derive(Debug)]
pub struct UseDef {
    pub db_name: String,
}

impl DOP for UseDef {
    fn cmd(self: &Self) -> DbCmd {
        DbCmd::Use
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for UseDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Use Db Name:{}", self.db_name)
    }
}

#[derive(Debug)]
pub struct InsertDef {
    pub table_name: String,
    pub cols: Vec<String>,
    pub datas: Vec<Vec<Expr>>,
}

impl DOP for InsertDef {
    fn cmd(self: &Self) -> DbCmd {
        DbCmd::Insert
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for InsertDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Insert Def:{:?}", self)
    }
}

pub fn parse_sql(sql: &str) -> Result<Box<dyn DOP>> {
    //process show
    if sql.starts_with("show database") {
        return Ok(Box::new(ShowDef {
            show_type: ShowType::DataBase,
        }));
    }

    if sql.starts_with("show table") {
        return Ok(Box::new(ShowDef {
            show_type: ShowType::Table,
        }));
    }

    if sql.starts_with("use") {
        let split: Vec<&str> = sql.split(|x| x == ' ' || x == ';').collect();
        if split.len() < 2 {
            bail!("invalid use statement: {}", sql)
        }
        let use_def = UseDef {
            db_name: split[1].to_string(),
        };
        return Ok(Box::new(use_def));
    }

    let dialect = MySqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql);

    match ast {
        Ok(mut data) => {
            if data.len() > 1 {
                bail!("too much sql :{}", sql);
            }
            let statement = data.pop().unwrap();

            let dop: Box<dyn DOP> = match statement {
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
                Statement::CreateTable { name, columns, .. } => {
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
                    table_name,
                    columns,
                    source,
                    ..
                } => {
                    let col_names: Vec<String> = columns
                        .iter()
                        .map(|x| String::from(x.value.as_str()))
                        .collect();
                    println!("[debug] col names:{:?}", col_names);

                    let t_name = String::from(table_name.0.get(0).unwrap().value.as_str());

                    let body = source.body;
                    match body {
                        SetExpr::Values(d) => {
                            let insert_def = InsertDef {
                                table_name: t_name,
                                cols: col_names,
                                datas: d.0,
                            };
                            Box::new(insert_def)
                        }
                        _ => {
                            bail!("error parse insert sql:{}", sql);
                        }
                    }
                }
                _ => {
                    bail!("unknown parse statement :{}", sql);
                }
            };

            Ok(dop)
        }
        Err(err) => {
            bail!("system error in parse statement :{}, error:{:?}", sql, &err)
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
