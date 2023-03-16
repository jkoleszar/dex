# Protos describing the Object Database (ODB)
@0x826aeea7c437f548;

struct ObjectId {
    id @0 :Data;
}

struct Object {
    blob @0 :Data;
}

interface Import {
    sendObject @0 (object :Object) -> ();
    # Send the provided object to the remote (importing) side.
    #
    # TODO: use 'stream' return type once supported.

    done @1 ();
    # Invoke once all objects have been sent.
}

interface Export {
    want @0 (id :ObjectId) -> ();
    # Specify an object that should be exported
    #
    # TODO: use 'stream' return type once supported.

    have @1 (id :ObjectId) -> ();
    # Specify an object already present on the remote (importing) side.
    #
    # TODO: use 'stream' return type once supported.

    begin @2 (import :Import);
    # Begin streaming objects to the remote (importing) side.
}

interface Odb {
    import @0 () -> (importer :Import);
    export @1 () -> (exporter :Export);
}