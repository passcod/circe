extern crate pretty_env_logger;
extern crate iron;
#[macro_use]
extern crate log;
extern crate logger;
extern crate odbc;
#[macro_use]
extern crate serde_json;

use iron::prelude::*;
use iron::headers::ContentType;
use iron::status;
use iron::method::Method;
use logger::Logger;
use odbc::{Environment, DataSource, Connected, Statement, Version3};
use odbc::ffi::SqlDataType;
use serde_json::map::Map;
use serde_json::Value;
use std::env;
use std::io::Read;
use std::process::exit;

enum Type {
    Float,
    Int,
    String,
}

fn make_env() -> Environment<Version3> {
    Environment::new()
        .expect("Can't create ODBC environment")
        .set_odbc_version_3()
        .expect("Can't use ODBCv3")
}

fn connect(oenv: &Environment<Version3>) -> DataSource<Connected> {
    let dsn = env::var("DSN").unwrap_or_else(|e| {
        error!("Cannot parse env var 'DSN': {}", e);
        exit(1);
    });

    DataSource::with_parent(oenv)
        .expect("Can't create Data Source")
        .connect_with_connection_string(&dsn)
        .expect("Can't connect to DNS")
}

fn get_types(s: &Statement<odbc::Executed, odbc::HasResult>, cols: u16) -> Vec<(String, Type)> {
    let mut types = vec![];
    for i in 1..cols {
        let desc = match s.describe_col(i) {
            Ok(c) => {
                (
                    c.name,
                    match c.data_type {
                        SqlDataType::SQL_DOUBLE => Type::Float,
                        SqlDataType::SQL_REAL => Type::Float,
                        SqlDataType::SQL_FLOAT => Type::Float,
                        SqlDataType::SQL_DECIMAL => Type::Float,
                        SqlDataType::SQL_NUMERIC => Type::Float,
                        SqlDataType::SQL_INTEGER => Type::Int,
                        SqlDataType::SQL_SMALLINT => Type::Int,
                        _ => Type::String
                    }
                )
            },
            Err(_) => ("unknown".into(), Type::String)
        };

        types.push(desc);
    }

    return types
}

fn do_query(conn: &mut DataSource<Connected>, q: String) -> Value {
    let stmt = Statement::with_parent(conn);
    if let Err(e) = stmt {
        return json!([
            { "Error": format!("Can't prepare statement: {}", e) }
        ]);
    }

    let run = stmt.unwrap().exec_direct(&q);
    if let Err(e) = run {
        return json!([
            { "Error": format!("Can't execute query: {}", e) }
        ]);
    }

    let result = run.unwrap();
    match result {
        odbc::NoData(_) => json!([]),
        odbc::Data(mut s) => {
            let cols = s.num_result_cols();
            if let Err(e) = cols {
                return json!([
                    { "Error": format!("Can't fetch results: {}", e) }
                ]);
            }
            let cols = cols.unwrap() as u16 + 1;
            let types = get_types(&s, cols);

            let mut rows = vec![];
            while let Some(mut cursor) = match s.fetch() {
                Ok(c) => c,
                Err(e) => return json!([
                    { "Error": format!("Can't fetch results: {}", e) }
                ])
            } {
                let mut row = Map::new();
                for (i, &(ref name, ref typ)) in types.iter().enumerate() {
                    let data = match typ {
                        &Type::Float => match cursor.get_data::<f64>(i as u16) {
                            Ok(f) => json!(f),
                            Err(e) => return json!([
                                { "Error": format!("Can't fetch data: {}", e) }
                            ])
                        },
                        &Type::Int => match cursor.get_data::<i64>(i as u16) {
                            Ok(n) => json!(n),
                            Err(e) => return json!([
                                { "Error": format!("Can't fetch data: {}", e) }
                            ])
                        },
                        &Type::String => match cursor.get_data::<String>(i as u16) {
                            Ok(s) => json!(s),
                            Err(e) => return json!([
                                { "Error": format!("Can't fetch data: {}", e) }
                            ])
                        }
                    };

                    row.insert(name.clone(), data);
                }

                rows.push(row);
            }

            json!(rows)
        }
    }
}

fn router(req: &mut Request) -> IronResult<Response> {
    if req.method == Method::Options {
        return Ok(Response::with((status::NoContent, "")));
    }

    if req.url.path() != vec![""] {
        return Ok(Response::with((status::NotFound, "")));
    }

    if req.method != Method::Post {
        return Ok(Response::with((status::MethodNotAllowed, "")));
    }

    let json = serde_json::from_reader(req.body.by_ref());
    if let Err(e) = json {
        return Ok(Response::with((status::BadRequest, format!("{}", e))));
    }

    let json: Value = json.unwrap();
    let mut queries: Vec<String> = vec![];
    if let Value::Array(qs) = json {
        for q in qs {
            if let Value::String(s) = q {
                queries.push(s);
            } else {
                return Ok(Response::with((status::InternalServerError, "Not an array of strings")));
            }
        }
    } else {
        return Ok(Response::with((status::InternalServerError, "Not a JSON array")));
    }

    fn respond(s: String) -> IronResult<Response> {
        let mut res = Response::with((status::Ok, s));
        res.headers.set(ContentType::json());
        Ok(res)
    }

    if queries.len() == 0 {
        return respond("[]".into());
    }

    let oenv = make_env();
    let mut conn = connect(&oenv);
    let mut results: Vec<Value> = vec![];
    for q in queries {
        results.push(do_query(&mut conn, q));
    }

    respond(json!(results).to_string())
}

fn main() {
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", format!("{}=info,logger=info", env!("CARGO_PKG_NAME")));
    }

    pretty_env_logger::init().unwrap();
    info!("{} v{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    let (logger_before, logger_after) = Logger::new(None);
    let mut chain = Chain::new(router);
    chain.link_before(logger_before);
    chain.link_after(logger_after);

    let port = env::var("PORT").unwrap_or("3000".into());
    info!("Listening on {}", port);
    Iron::new(chain)
        .http(format!("0.0.0.0:{}", port))
        .expect("Can't start server");
}
