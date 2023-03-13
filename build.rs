fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("proto/")
        .default_parent_module(vec!["proto".into()])
        .file("proto/odb.capnp")
        .run()
        .expect("capnpc failed");
}
