// use std::path::Path;
//
// use kvs_engine::Engine;
// use kvs_server::writer::spawn;

fn main() {
    println!("kvs-server");
    // if let Ok(engine) = Engine::open_with(Path::new(".db"), kvs_engine::FsyncPolicy::Always) {
    //     let (kv_handle, join_handle) = spawn(engine, 1024);
    //
    //     // handle.set("a".into(), "b".into()).await.expect("set");
    //
    //     drop(kv_handle);
    //     join_handle.join().expect("writer thread must exit cleanly");
    // }
}
