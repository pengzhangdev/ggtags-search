//extern crate clap;
use clap::{Arg, App, SubCommand};
use rusqlite::{Connection, Result};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::thread;
use std::sync::Arc;
use threadpool::ThreadPool;

// testcases/open_posix_testsuite/conformance/interfaces/pthread_mutexattr_setprotocol/3-2.c:23: int main(void)

#[derive(Debug)]
struct OpengrokArgs {
    config : String,
    project : String,
    define : String,
    reference : String,
    text : String,
    filename : String,
    workfile : String,
}

fn uncompress(name : &String, inbuf : &String) -> String {
    let mut result = String::new();

    let mut next_token : usize = 0;
    for i in 0 .. inbuf.chars().count() {
        if i < next_token {
            continue
        }
        //println!("{}", inbuf.chars().nth(i).unwrap());
        let c = inbuf.chars().nth(i).unwrap();
        if c == '@' {
            let mut spaces = 0;
            next_token = i + 1;
            let next = inbuf.chars().nth(next_token).unwrap();
            match next {
                '@' => result.push('@'),
                'n' => result.push_str(name),
                't' => result.push_str("typedef"),
                'd' => result.push_str("define"),
                '{' => {
                    next_token += 1;
                    let mut m = inbuf.chars().nth(next_token).unwrap();
                    //println!("m = {}", m);
                    while m != '\0' && m.is_digit(10) {
                        spaces = spaces * 10 + m.to_digit(10).unwrap();
                        next_token += 1;
                        m = inbuf.chars().nth(next_token).unwrap();
                    }
                }
                '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' => {
                    spaces = next.to_digit(10).unwrap();
                }
                _ => {println!("Failed to parse");}
            }
            result.push_str(&" ".repeat(spaces.try_into().unwrap()));
            next_token += 1; // skip
        } else {
            result.push(c);
        }
    }

    return result;
}


#[derive(Debug, PartialEq)]
struct XrefInfo {
    filepath : String,
    linum : i64,
    line : String,
    priority : i64,
}

fn pattern_query(key : &str, pattern : bool) -> String {
    if pattern { 
        format!(" LIKE '%{}%'", key)
    } else {
        format!(" = '{}'", key)
    }
}

fn get_fullpath(dbpath: &str, name : &str) -> String {
    let db = Path::new(dbpath).join(name);
    db.canonicalize().unwrap().to_str().unwrap().to_string()
}

fn sqlite_get_file(dbpath : &str, key : String, pattern : bool) -> Result<Vec<String>> {
    let sqlquery = format!("SELECT dat FROM db WHERE KEY {}", pattern_query(key.trim(), pattern));
    //println!("{}", sqlquery);
    let conn = Connection::open(get_fullpath(dbpath, "GPATH"))?;
    let mut stmt = conn.prepare(&sqlquery)?;
    let mut files = stmt.query_map([], |row| {
        let value : String = row.get(0)?;
        Ok(value)
    })?;
    let mut result : Vec<String> = Vec::new();
    for f in files {
        //println!("file : {}", f.unwrap());
        result.push(f.unwrap());
    }
    
    Ok(result)
}

fn sqlite_get_file2(dbpath : &str, key : String, pattern : bool) -> Result<Vec<String>> {
    let sqlquery = format!("SELECT KEY FROM db WHERE KEY {}", pattern_query(key.trim(), pattern));
    //println!("{}", sqlquery);
    let conn = Connection::open(get_fullpath(dbpath, "GPATH"))?;
    let mut stmt = conn.prepare(&sqlquery)?;
    let mut files = stmt.query_map([], |row| {
        let value : String = row.get(0)?;
        Ok(value)
    })?;
    let mut result : Vec<String> = Vec::new();
    for f in files {
        //println!("file : {}", f.unwrap());
        result.push(f.unwrap());
    }
    
    Ok(result)
}

fn sqlite_search_file(dbpath: &str, key: String) -> Result<Vec<XrefInfo>> {
    let files = sqlite_get_file2(&dbpath, key.clone(), true).unwrap();
    let mut result : Vec<XrefInfo> = Vec::new();
    //println!("{:#?}", files);
    for f in files {
        let path = get_fullpath(&dbpath, &f);
        result.push(XrefInfo {
            line : format!("{}:{}:{}", path, 1, key),
            linum : 1,
            filepath : path,
            priority : 0
        })
    }

    return Ok(result);
}
 
fn sqlite_get_define(dbpath : &str, key : String, pattern : bool) -> Result<Vec<XrefInfo>> {
    let sqlquery = format!("SELECT DISTINCT key,dat FROM db WHERE KEY {}", pattern_query(&key, pattern));
    let conn = Connection::open(get_fullpath(dbpath, "GTAGS"))?;
    let mut stmt = conn.prepare(&sqlquery)?;
    let pool = ThreadPool::new(num_cpus::get());
    let (tx, rx): (Sender<XrefInfo>, Receiver<XrefInfo>) = mpsc::channel();
    let XrefInfos = stmt.query_map([], |row| {
        let thread_tx = tx.clone();
        let value  : String = row.get(1)?;
        let key : String = row.get(0)?;
        {
            let dbpath = dbpath.to_owned();
            //println!("define {} = {}", key, uncompress(&key, &value));
            pool.execute(move || {
                let line = uncompress(&key, &value);
                let v : Vec<&str> = line.split(&key).collect();
                let path = &sqlite_get_file(&dbpath, v[0].to_string(), false).unwrap()[0];
                let path = get_fullpath(&dbpath, path);
                let linum = v[1].trim().split(" ").collect::<Vec<&str>>()[0].trim();
                let l = line.replace(&format!("{}{} {}", v[0], key, linum), &(path.clone() + ":" + linum + ":"));

                thread_tx.send( XrefInfo { 
                    line : l,
                    linum : linum.parse::<i64>().unwrap(),
                    filepath : path.to_string(),
                    priority : 0
                }).unwrap();
            });
            Ok(())
        }
    })?;

    for xf in XrefInfos { // trigger the closure

    }
    
    drop(tx);
    let mut result : Vec<XrefInfo> = Vec::new();
    for info in rx {
        result.push(info);
    }

    Ok(result)
}

fn sqlite_get_symbol_line(reader : &mut std::io::BufReader<std::fs::File>, offset : i32) -> String {
    let mut line = String::new();
    for n in 1 .. offset {
        reader.read_line(&mut line);
    }
    line.clear();
    reader.read_line(&mut line);
    return line.trim_end().to_string();
}

fn sqlite_get_symbol(dbpath : &str, key : String, pattern : bool) -> Result<Vec<XrefInfo>> {
    //let sy_time = SystemTime::now();
    let sqlquery = format!("SELECT DISTINCT key,dat FROM db WHERE KEY {}", pattern_query(&key, pattern));
    let conn = Connection::open(get_fullpath(dbpath, "GRTAGS"))?;
    let mut stmt = conn.prepare(&sqlquery)?;
    let pool = ThreadPool::new(num_cpus::get());
    let (tx, rx): (Sender<XrefInfo>, Receiver<XrefInfo>) = mpsc::channel();
    let XrefInfos = stmt.query_map([], |row| {
        let thread_tx = tx.clone();
        let value : String = row.get(1).unwrap();
        let key : String = row.get(0).unwrap();
        {
            //println!("{:?}  key {:?}", v, key);
                //let abspath = Path::new(dbpath).join(path).canonicalize().unwrap();
            let dbpath = dbpath.to_owned(); // own dbpath
            pool.execute(move || {
                            //println!("symbol {} = {}", key, uncompress(&key, &value));
                let s = uncompress(&key, &value);
                let v : Vec<&str> = s.split(&key).collect();
                let path = &sqlite_get_file(&dbpath, v[0].to_string(), false).unwrap()[0];
                let path = get_fullpath(&dbpath, path);
                let mut reader = std::io::BufReader::new(std::fs::File::open(&path).unwrap());
                let v : Vec<&str> = v[1].trim().split(",").collect();
                let mut next : i64 = 0;

                for o in v {
                    if o.find('-') != None {
                        let ov : Vec<&str> = o.split("-").collect();
                        let oo = ov[0];
                        let os = ov[1];
                        next += oo.parse::<i64>().unwrap();
                        let line = sqlite_get_symbol_line(&mut reader, oo.parse::<i32>().unwrap());
                        thread_tx.send(XrefInfo {
                            filepath : format!("{}:{}", path, next),
                            linum : next,
                            line : format!("{}:{}:{}", path, next, line),
                            priority : 0,
                        }).unwrap();
                        for i in 1 .. (os.parse::<i32>().unwrap() + 1) {
                            //println!("line {} oo = {} i = {}", path, oo.parse::<i64>().unwrap(), 1);
                            next += i as i64;
                            let line = sqlite_get_symbol_line(&mut reader, 1);
                            thread_tx.send(XrefInfo {
                                filepath : format!("{}:{}", path, next),
                                line : format!("{}:{}:{}", path, next, line),
                                linum : next,
                                priority : 0,
                            }).unwrap();
                        }
                    } else {
                        next += o.parse::<i64>().unwrap();
                        let line = sqlite_get_symbol_line(&mut reader, o.parse::<i32>().unwrap());
                        //println!("tx send");
                        thread_tx.send(XrefInfo {
                            filepath : format!("{}:{}", path, next),
                            line : format!("{}:{}:{}", path, next, line),
                            linum : next,
                            priority : 0,
                        }).unwrap();
                    }
                }
            });
        }
        Ok(())
    })?;
    for info in XrefInfos {
        //println!("xrefs {:?}", info);
    }
    drop(tx);
    let mut result : Vec<XrefInfo> = Vec::new();
    for xref in rx {
        //println!("rx receive");
        result.push(xref);
    }
    //let duration = SystemTime::now().duration_since(sy_time).unwrap();
    //println!("Time cost in sqlite_get_symbol : {:?} s", duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9);
    Ok(result)
}

fn sqlite_get_text(dbpath : &str, key : String) -> Result<Vec<XrefInfo>>{
    let mut symbol_result = sqlite_get_symbol(dbpath, key.clone(), true).unwrap();
    let mut define_result = sqlite_get_define(dbpath, key, true).unwrap();
    symbol_result.append(&mut define_result);
    Ok(symbol_result)
}

fn levenshtein_distance(s : &str, t : &str, ignore_case : bool) -> usize {
    if ignore_case {
        levenshtein::levenshtein(&s.to_string().to_lowercase(), &t.to_string().to_lowercase())
    } else {
        levenshtein::levenshtein(s, t)
    }
}

fn cal_prio(target : &str, wd : &str) -> i64 {
    let mut matched : i64 = 0;
    if wd.len() > 0 {
        let count = std::cmp::min(wd.len(), target.len());
        while matched < count as i64 {
            if wd.chars().nth(matched as usize) == target.chars().nth(matched as usize) {
                break;
            }
            matched += 1;
        }

        if matched != 0 {  // sufix + 100
            
            matched = matched * 10 - levenshtein_distance(Path::new(&wd).file_name().unwrap().to_str().unwrap(), 
                                                            Path::new(&target).file_name().unwrap().to_str().unwrap(), false) as i64;
        } else {
            matched = matched * 10 - levenshtein_distance(&wd, &target, false) as i64
        }
    } else {
        matched = target.len() as i64;
    }

    if target.to_lowercase().find("/test") != None {
        matched -= 100;
    }

    return -matched;
}

fn sort_with_prio(xrefs : Vec<XrefInfo>, wd : String) -> Vec<XrefInfo> {
    //println!("{}", levenshtein::levenshtein("aca", "bbbc"))
    //let sy_time = SystemTime::now();
    let mut result : Vec<XrefInfo> = Vec::new();
    let pool = ThreadPool::new(num_cpus::get());
    let (tx, rx): (Sender<XrefInfo>, Receiver<XrefInfo>) = mpsc::channel();
    for mut xref in xrefs {
        let wd = wd.clone();
        let thread_tx = tx.clone();
        pool.execute(move || {
            xref.priority = cal_prio(&xref.filepath, &wd) * 1000000 + xref.linum;
            thread_tx.send(xref).unwrap();
        });
        //result.push(xref);
    }
    drop(tx);
    for xref in rx {
        result.push(xref);
    }
    result.sort_by(|a, b| b.priority.cmp(&a.priority));
    result.reverse();
    result.dedup();
    //let duration = SystemTime::now().duration_since(sy_time).unwrap();
    //println!("Time cost in sort_with_prio : {:?} s", duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9);
    return result;
}

fn parse_args() -> OpengrokArgs {
    let matches = App::new("opengrok-search")
                        .version("1.0")
                        .author("Werther Zhang")
                        .about("Search ggtags database. All the source codes are in Apache License v2.0.")
                        .arg(Arg::with_name("config")
                                .short("c")
                                .long("config")
                                .value_name("FILE")
                                .help("Set Opengrok config file path")
                                .takes_value(true)
                                .required(false))
                        .arg(Arg::with_name("project")
                                .short("p")
                                .long("project")
                                .value_name("PROJECT")
                                .help("opengrok project name")
                                .takes_value(true)
                                .required(true))
                        .arg(Arg::with_name("define")
                                .short("d")
                                .long("define")
                                .value_name("SYMBOL")
                                .help("search symbol with define")
                                .takes_value(true))
                        .arg(Arg::with_name("reference")
                                .short("r")
                                .long("reference")
                                .value_name("SYMBOL")
                                .help("search symbol with reference")
                                .takes_value(true))
                        .arg(Arg::with_name("text")
                                .short("t")
                                .long("text")
                                .value_name("SYMBOL")
                                .help("search symbol with full text")
                                .takes_value(true))
                        .arg(Arg::with_name("file")
                                .short("f")
                                .long("file")
                                .value_name("FILENAME")
                                .help("search filename")
                                .takes_value(true))
                        .arg(Arg::with_name("workfile")
                                .short("w")
                                .long("workfile")
                                .value_name("WORKFILE")
                                .help("current file search from")
                                .takes_value(true))
                        .get_matches();

    return OpengrokArgs {
    config :matches.value_of("config").unwrap().to_string(),
    project : matches.value_of("project").unwrap_or("").to_string(),
    define : matches.value_of("define").unwrap_or("").to_string(),
    reference : matches.value_of("reference").unwrap_or("").to_string(),
    text : matches.value_of("text").unwrap_or("").to_string(),
    filename : matches.value_of("file").unwrap_or("").to_string(),
    workfile : matches.value_of("workfile").unwrap_or("").to_string(),
    }
}

fn output_xrefs(xrefs : &Vec<XrefInfo>) {
    for xref in  xrefs {
        println!("{}", xref.line);
    }
}

fn main() {
    let sy_time = SystemTime::now();
    let opengrok_args = parse_args();
    println!("{:?}", opengrok_args);
    //sqlite_get_define("sqlite3BtreeSetPageSize".to_string());
    //println!("define {:?}", sort_with_prio(sqlite_get_symbol(opengrok_args.define, false).unwrap(), opengrok_args.workfile));
    //println!("{} , {}", levenshtein_distance("aaa", "Abb", false), levenshtein_distance("aaa", "Abb", true));
    //println!("symbol {:?}", sqlite_get_symbol("SQLITE_IOERR_READ".to_string(), false));
    //let r = sqlite_get_file("196".to_string(), false);
    //println!("{:?}", r.unwrap());
    //sqlite_get_symbol("WalIndexHdr".to_string());
    //println!("text  {:?}", sqlite_get_text("PackageManagerSer".to_string()));
    //let result = uncompress(&"196 @n 48397 @t struct @n @n;".to_string(), &"WalIndexHdr".to_string());
    //println!("result : {}", result);
    //println!("args {:?}", opengrok_args);
    let dbpath = opengrok_args.project;
    match (opengrok_args.define.len() != 0, opengrok_args.reference.len() != 0, opengrok_args.text.len() != 0, opengrok_args.filename.len() != 0) {
        (true, false, false, false) => {
            output_xrefs(&sort_with_prio(sqlite_get_define(&dbpath, opengrok_args.define, false).unwrap(), opengrok_args.workfile));
        }
        (false, true, false, false) => {
            output_xrefs(&sort_with_prio(sqlite_get_symbol(&dbpath, opengrok_args.reference, false).unwrap(), opengrok_args.workfile));
        }
        (false, false, true, false) => {
            output_xrefs(&sort_with_prio(sqlite_get_text(&dbpath, opengrok_args.text).unwrap(), opengrok_args.workfile));
        }
        (false, false, false, true) => {
            output_xrefs(&sort_with_prio(sqlite_search_file(&dbpath, opengrok_args.filename).unwrap(), opengrok_args.workfile))
        }
        _ => {
            println!("define len {}, reference len {}, text len {}", 
                        opengrok_args.define.len(), opengrok_args.reference.len(), opengrok_args.text.len());
        }
    }
    let duration = SystemTime::now().duration_since(sy_time).unwrap();
    println!("Time cost: {:?} s", duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9);

}
