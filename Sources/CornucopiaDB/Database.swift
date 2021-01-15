//
//  Cornucopia – (C) Dr. Lauer Information Technology
//
import YapDatabase
import ULID
import os.signpost

public  func CC_measureSyncBlock(_ title: String = "", block: @escaping( () -> ())) {

    let startTime = CFAbsoluteTimeGetCurrent()
    block()
    let timeElapsed = CFAbsoluteTimeGetCurrent() - startTime
    print("\(title):: Time: \(timeElapsed)")
}


private var Log = OSLog(subsystem: "de.vanille.Cornucopia.DB", category: "Database")

public extension CornucopiaDB {

    typealias NoMetaData = Void
    typealias Connection = YapDatabaseConnection
    typealias Options = YapDatabaseOptions
    typealias ReadTransaction = YapDatabaseReadTransaction
    typealias ReadWriteTransaction = YapDatabaseReadWriteTransaction

    class Database {

        let name: String
        let db: YapDatabase
        var connections: [String: YapDatabaseConnection]
        var collections: [String: AnyObject]
        var views: [String: AnyObject]
        var defaultConnection: Connection { self.connection() }

        /// Creates a new database with the specified `name` in the user's documents directory, or, if you provide an absolute path, elsewhere.
        /// If you don't specify the `name`, the database gets created as `default.db` (in the user's documents directory).
        /// If the provided name is an absolute path, it gets used as is.
        public convenience init(name: String = "default.db") {
            if name.hasPrefix("/") {
                let url = URL(fileURLWithPath: name)
                self.init(url: url)
            } else {
                guard let docdir = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first else {
                    fatalError("Can't compute ~/Documents")
                }
                let path = docdir.appendingPathComponent(name, isDirectory: false).path
                let url = URL(fileURLWithPath: path)
                self.init(url: url)
            }
        }

        public init(url: URL) {
            self.name = url.deletingPathExtension().lastPathComponent
            guard let db = YapDatabase(url: url) else {
                fatalError("Can't open database at \(url)")
            }
            self.db = db
            self.connections = [:]
            self.collections = [:]
            self.views = [:]
        }

        public func enableRelationships() {
            let relationships = YapDatabaseRelationship()
            let name = Extension.Relationships.rawValue
            guard db.register(relationships, withName: name) else {
                os_log("Could not register extension '%s'", log: Log, type: .info, name)
                return
            }
            os_log("Registered extension '%s'", log: Log, type: .default, name)
        }

        public func connection(consumer: String = "") -> Connection {
            guard let connection = self.connections[consumer] else {
                #if false
                let config = YapDatabaseConnectionConfig()
                config.objectCacheLimit = 100000
                config.metadataCacheLimit = 100000
                config.metadataCacheEnabled = true
                config.objectCacheEnabled = true
                let newConnection = self.db.newConnection(config)
                #else
                let newConnection = self.db.newConnection()
                #endif
                self.connections[consumer] = newConnection
                return newConnection
            }
            return connection
        }

        /// Returns a new codable collection that only stores items of type `OT` without metadata.
        public func codableCollection<OT: Codable>(name: String = String(describing: OT.self),
                                                   keyed via: @escaping CodableCollection<OT, NoMetaData>.KeyFunction = { _ in ULID().ulidString },
                                                   compressed: NSData.CompressionAlgorithm? = nil) -> CodableCollection<OT, NoMetaData> {
            let collection = CodableCollection<OT, NoMetaData>(name: name, keyed: via, compressed: compressed)
            self.register(collection: collection) // defined in CodableCollection
            self.collections[collection.name] = collection
            return collection
        }

        /// Returns a new codable collection that only stores items of type `OT` and metadata of type `MT`.
        public func codableCollection<OT: Codable, MT: Codable>(name: String = String(describing: OT.self),
                                                                keyed via: @escaping CodableCollection<OT, MT>.KeyFunction = { _ in ULID().ulidString },
                                                                compressed: NSData.CompressionAlgorithm? = nil,
                                                                meta: @escaping CodableCollection<OT, MT>.MetaFunction,
                                                                metaCompression: NSData.CompressionAlgorithm? = nil) -> CodableCollection<OT, MT> {
            let collection = CodableCollection<OT, MT>(name: name, keyed: via, compressed: compressed, meta: meta, metaCompression: metaCompression)
            self.register(collection: collection) // defined in CodableCollection
            self.collections[collection.name] = collection
            return collection
        }

        /// Returns a virtual collection representing a number of actual collections storing items of type `OT`.
        public func multiCollection<OT: Codable>(name: String) -> MultiCollection<OT, NoMetaData> {
            let collection = MultiCollection<OT, NoMetaData>(name: name, db: self)
            return collection
        }

        //MARK:- Internal

        @discardableResult
        internal func register<OT, MT>(view: View<OT, MT>) -> View<OT, MT> {
            if let existingView = self.views[view.name] {
                return existingView as! View<OT, MT>
            }

            os_signpost(.begin, log: Log, name: "Register Database View", "%@", view.name); defer { os_signpost(.end, log: Log, name: "Register Database View") }
            guard self.db.register(view.dbView, withName: view.name) else { fatalError("Could not register extension \(view.name)") }
            view.database = self
            self.views[view.name] = view as AnyObject
            return view
        }

        internal func register<OT, MT>(view: View<OT, MT>, then: @escaping((Bool) -> ())) {
            if let _ = self.views[view.name] {
                then(true)
            }
            os_signpost(.begin, log: Log, name: "Register Database View", "%@", view.name); defer { os_signpost(.end, log: Log, name: "Register Database View") }
            self.db.asyncRegister(view.dbView, withName: view.name) { success in
                guard success else { fatalError("Could not register extension \(view.name)") }
                view.database = self
                self.views[view.name] = view as AnyObject
                then(true)
            }
        }
    }
}

public extension CornucopiaDB.Database {

    func enumerateCollectionNames(_ block: (String)->()) {
        self.defaultConnection.read { t in
            t.enumerateCollectionNames { name, _ in
                block(name)
            }
        }
    }
}
