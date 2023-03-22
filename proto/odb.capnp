# Protos describing the Object Database (ODB)
@0x826aeea7c437f548;
using Json = import "/capnp/json.capnp";

struct ObjectId {
    id @0 :Data $Json.hex;
}

struct Tree {
    struct Entry {
        enum Kind {
            dir @0;
            file @1;
            symlink @2;
        }

        kind @0 :Kind;
        oid @1 :ObjectId;
        name @2 :Text;
    }
    entries @0 :List(Entry);
}

struct Object {
    blob @0 :Data $Json.base64;
    tree @1 :Tree;
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
