# Protos describing the Object Database (ODB)
@0x826aeea7c437f548;
using Json = import "/capnp/json.capnp";

struct ObjectId {
    id @0 :Data $Json.hex;
}

struct Stat {
    enum Kind {
        block @0;
        char @1;
        fifo @2;
        regular @3;
        dir @4;
        symlink @5;
    }
    kind @0 :Kind;
    # st_dev omitted.
    # st_ino omitted.
    stMode @1 :UInt32;
    stNlink @2 :UInt64;
    stUid @3 :UInt32;
    stGid @4 :UInt32;
    deviceMajor @5 :UInt32;
    deviceMinor @6 :UInt32;
    stSize @7 :UInt64;
    # st_blksize omitted.
    # st_blocks omitted.
    # st_atime omitted.
    # st_atime_nsec omitted.
    stMtime @8 :UInt64;
    stMtimeNsec @9 :UInt32;
    stCtime @10 :UInt64;
    stCtimeNsec @11 :UInt32;
}
struct Tree {
    struct Entry {
        oid @0 :ObjectId;
        name @1 :Text;
        stat @2 :Stat;
    }
    entries @0 :List(Entry);
}
struct TreeRoot {
    tree @0 :Tree;
    stat @1 :Stat;
}
struct Object {
    union {
        blob @0 :Data $Json.base64;
        tree @1 :Tree;
        treeRoot @2 :TreeRoot;
    }
}

interface Import {
    sendObject @0 (object :Object) -> (self: Import);
    # Send the provided object to the remote (importing) side.
    #
    # TODO: use 'stream' return type once supported.

    done @1 (self: Import);
    # Invoke once all objects have been sent.
}

interface Export {
    want @0 (id :ObjectId) -> (self: Export);
    # Specify an object that should be exported
    #
    # TODO: use 'stream' return type once supported.

    have @1 (id :ObjectId) -> (self: Export);
    # Specify an object already present on the remote (importing) side.
    #
    # TODO: use 'stream' return type once supported.

    begin @2 (import :Import);
    # Begin streaming objects to the remote (importing) side.
}

interface ExportFactory {
    new @0 () -> (export :Export);
}
