//
//  Cornucopia – (C) Dr. Lauer Information Technology
//
import YapDatabase
import MessagePacker
import ULID

import Compression
import OSLog

fileprivate var logger = OSLog(subsystem: "de.vanille.Cornucopia.DB", category: "CodableCollection")

public extension CornucopiaDB {

    ///TBD
    class CodableCollection<OT: Codable, MT> {

        public typealias Key = String
        public typealias KeyFunction = (OT) -> Key
        public typealias MetaFunction = (OT) -> MT

        public let name: String
        public let keyFunction: KeyFunction
        public let itemCompression: NSData.CompressionAlgorithm?
        public let metaFunction: MetaFunction?
        public let metaCompression: NSData.CompressionAlgorithm?
        internal weak var db: CornucopiaDB.Database!

        /// Creates a codable collection that only stores items of type `OT` without metadata.
        ///
        /// Typically you supply a `KeyFunction` (e.g. a `KeyPath`) that derives the unique key for every item
        /// based on one item's property (or a combination of properties). Should you chose to not supply a
        /// `KeyFunction`, a `ULID` will be assigned as a unique key for every persisted item.
        internal init(name: String = String(describing: OT.self),
                      keyed via: @escaping KeyFunction = { _ in ULID().ulidString },
                      compressed: NSData.CompressionAlgorithm? = nil) {

            self.name = name
            self.keyFunction = via
            self.itemCompression = compressed
            self.metaFunction = nil
            self.metaCompression = nil
        }

        /// Creates a codable collection that only stores items of type `OT` and metadata of type `MT`.
        ///
        /// Typically you supply a `KeyFunction` (e.g. a `KeyPath`) that derives the unique key for every item
        /// based on one item's property (or a combination of properties). Should you chose to not supply a
        /// `KeyFunction`, a `ULID` will be assigned as a unique key for every persisted item.
        internal init(name: String = String(describing: OT.self),
                    keyed via: @escaping KeyFunction = { _ in ULID().ulidString },
                    compressed: NSData.CompressionAlgorithm? = nil,
                    meta: @escaping MetaFunction,
                    metaCompression: NSData.CompressionAlgorithm? = nil) {

            self.name = name
            self.keyFunction = via
            self.itemCompression = compressed
            self.metaFunction = meta
            self.metaCompression = metaCompression
        }

        /// Returns an item for key, if existing.
        /// If a transaction is given, it will be used.
        /// If a connection is given, a transaction will be created.
        /// If neither a transaction nor a connection is given, a transaction will on the default connection will be created.
        public func item(for key: String,
                         via transaction: YapDatabaseReadTransaction? = nil,
                         on connection: YapDatabaseConnection? = nil) -> OT? {

            precondition(db != nil, "Collection is not registered with a database yet")
            precondition((transaction != nil && connection == nil) ||
                            (connection != nil && transaction == nil) ||
                            (transaction == nil && connection == nil), "Unsupported combination of non-nil transaction and non-nil connection")

            var object: OT? = nil

            let block: (YapDatabaseReadTransaction) -> () = { t in
                object = t.object(forKey: key, inCollection: self.name) as? OT
            }

            guard let t = transaction else {
                let connection = connection ?? self.db.defaultConnection
                connection.read(block)
                return object
            }
            block(t)
            return object
        }

        /// Returns the number of items in this collection.
        public func numberOfItems(
            via transaction: YapDatabaseReadWriteTransaction? = nil,
            on connection: YapDatabaseConnection? = nil) -> Int {

            precondition(db != nil, "Collection is not registered with a database yet")
            precondition((transaction != nil && connection == nil) ||
                            (connection != nil && transaction == nil) ||
                            (transaction == nil && connection == nil), "Unsupported combination of non-nil transaction and non-nil connection")

            var n: Int = 0

            let block: (YapDatabaseReadTransaction) -> () = { t in
                let u = t.numberOfKeys(inCollection: self.name)
                n = Int(u)
            }
            guard let t = transaction else {
                let connection = connection ?? self.db.defaultConnection
                connection.read(block)
                return n
            }
            block(t)
            return n
        }


        /// Returns an item for the specified `key`, if existing, otherwise `nil`.
        public subscript(key: String, transaction: YapDatabaseReadTransaction? = nil) -> OT? { return self.item(for: key, via: transaction) }

        /// Persists an item and (optionally) metadata.
        /// If a transaction is given, it will be used.
        /// If a connection is given, a transaction will be created.
        /// If neither a transaction nor a connection is given, a transaction will on the default connection will be created.
        /// The combination of a non-nil transaction _and_ a non-nil connection is invalid.
        ///
        /// NOTE: Since the `meta` argument is optional, you might end up with `nil` values in a `CodableCollection` that has a non-Void
        ///       meta type defined. This is currently by design. It remains to be seen whether we should a) forbid it through compiler rules
        ///       and/or emit a runtime warning.
        ///
        public func persist(item: OT,
                            meta: MT? = nil,
                            via transaction: YapDatabaseReadWriteTransaction? = nil,
                            on connection: YapDatabaseConnection? = nil) {

            precondition(db != nil, "Collection is not registered with a database yet")
            precondition((transaction != nil && connection == nil) ||
                            (connection != nil && transaction == nil) ||
                            (transaction == nil && connection == nil), "Unsupported combination of non-nil transaction and non-nil connection")

            let block: (YapDatabaseReadWriteTransaction) -> () = { t in
                let k = self.keyFunction(item)
                let m: MT? = meta ?? {
                    guard let metaFunction = self.metaFunction else { return nil }
                    return metaFunction(item)
                }()
                if let meta = m {
                    //print("\(self): Persisting \(item) with key '\(k)' and meta '\(meta)'")
                    t.setObject(item, forKey: k, inCollection: self.name, withMetadata: meta)
                } else {
                    //print("\(self): Persisting \(item) with key '\(k)'")
                    t.setObject(item, forKey: k, inCollection: self.name)
                }
            }

            guard let t = transaction else {
                let connection = connection ?? self.db.defaultConnection
                connection.readWrite(block)
                return
            }
            block(t)
        }

        /// Removes the specified `item` from this collection.
        /// NOTE: For this to work, the key function must derive the key from the model object.
        public func removeItem(_ item: OT, via transaction: YapDatabaseReadWriteTransaction? = nil, on connection: YapDatabaseConnection? = nil) {
            precondition(db != nil, "Collection is not registered with a database yet")
            precondition((transaction != nil && connection == nil) ||
                            (connection != nil && transaction == nil) ||
                            (transaction == nil && connection == nil), "Unsupported combination of non-nil transaction and non-nil connection")

            let block: (YapDatabaseReadWriteTransaction) -> () = { t in
                let key = self.keyFunction(item)
                guard t.hasObject(forKey: key, inCollection: self.name) else {
                    os_log("remove: item with key '%s' not found in CodableCollection '%s'", log: logger, type: .info, key, self.name)
                    return
                }
                t.removeObject(forKey: key, inCollection: self.name)
            }
            guard let t = transaction else {
                let connection = connection ?? self.db.defaultConnection
                connection.readWrite(block)
                return
            }
            block(t)
        }

        /// Removes the item specified by its `key` from this collection.
        public func removeItem(for key: String, via transaction: YapDatabaseReadWriteTransaction? = nil, on connection: YapDatabaseConnection? = nil) {
            precondition(db != nil, "Collection is not registered with a database yet")
            precondition((transaction != nil && connection == nil) ||
                            (connection != nil && transaction == nil) ||
                            (transaction == nil && connection == nil), "Unsupported combination of non-nil transaction and non-nil connection")

            let block: (YapDatabaseReadWriteTransaction) -> () = { t in
                guard t.hasObject(forKey: key, inCollection: self.name) else {
                    os_log("remove: item with key '%s' not found in CodableCollection '%s'", log: logger, type: .info, key, self.name)
                    return
                }
                t.removeObject(forKey: key, inCollection: self.name)
            }
            guard let t = transaction else {
                let connection = connection ?? self.db.defaultConnection
                connection.readWrite(block)
                return
            }
            block(t)
        }

        /// Removes all items in this collection.
        public func removeAll(via transaction: YapDatabaseReadWriteTransaction? = nil, on connection: YapDatabaseConnection? = nil) {
            precondition(db != nil, "Collection is not registered with a database yet")
            precondition((transaction != nil && connection == nil) ||
                            (connection != nil && transaction == nil) ||
                            (transaction == nil && connection == nil), "Unsupported combination of non-nil transaction and non-nil connection")

            let block: (YapDatabaseReadWriteTransaction) -> () = { t in
                t.removeAllObjects(inCollection: self.name)
            }
            guard let t = transaction else {
                let connection = connection ?? self.db.defaultConnection
                connection.readWrite(block)
                return
            }
            block(t)
        }

        /// Returns a view configured for the elements in this collection.
        public func view(name: String? = nil,
                    versionTag: String? = nil,
                    persistent: Bool = true,
                    grouping: CornucopiaDB.AutoView<OT, MT>.Grouping,
                    sorting: CornucopiaDB.AutoView<OT, MT>.ViewSortFunction) -> CornucopiaDB.AutoView<OT, MT> {

            precondition(db != nil, "Collection '\(self.name)' is not registered with a database yet")

            let view = CornucopiaDB.AutoView<OT, MT>(name: name ?? "\(self.name).view",
                                                 versionTag: versionTag,
                                                 persistent: persistent,
                                                 allowedCollections: Set([self.name]),
                                                 grouping: grouping,
                                                 sorting: sorting)
            self.db.register(view: view)
            return view
        }

        /// Returns a manual view configured for the elements in this collection.
        public func manualView(name: String? = nil,
                         versionTag: String? = nil,
                         persistent: Bool = true) -> CornucopiaDB.ManualView<OT, MT> {

            precondition(db != nil, "Collection '\(self.name)' is not registered with a database yet")

            let view = CornucopiaDB.ManualView<OT, MT>(name: name ?? "\(self.name).view",
                                                     versionTag: versionTag,
                                                     persistent: persistent,
                                                     allowedCollections: Set([self.name]))
            self.db.register(view: view)
            return view
        }

        /// Returns a view configured for the elements in this and other collections of the same type
        public func multiView(name: String,
                         versionTag: String? = nil,
                         persistent: Bool = true,
                         collections: [CodableCollection<OT, MT>],
                         grouping: CornucopiaDB.AutoView<OT, MT>.Grouping,
                         sorting: CornucopiaDB.AutoView<OT, MT>.ViewSortFunction) -> CornucopiaDB.AutoView<OT, MT> {

            precondition(db != nil, "Collection '\(self.name)' is not registered with a database yet")

            var allowedCollections = collections.map(\.name)
            allowedCollections.append(self.name)

            let view = CornucopiaDB.AutoView<OT, MT>(name: name,
                                                     versionTag: versionTag,
                                                     persistent: persistent,
                                                     allowedCollections: Set(allowedCollections),
                                                     grouping: grouping,
                                                     sorting: sorting)
            self.db.register(view: view)
            return view
        }
    }
}

//MARK: - Registration
internal extension CornucopiaDB.Database {

    @discardableResult
    func register<OT, MT>(collection: CornucopiaDB.CodableCollection<OT, MT>) -> CornucopiaDB.CodableCollection<OT, MT> {
        collection.db = self
        self.registerSerializers(OT.self, for: collection)
        return collection
    }

    @discardableResult
    func register<OT, MT>(collection: CornucopiaDB.CodableCollection<OT, MT>) -> CornucopiaDB.CodableCollection<OT, MT> where MT: Codable {
        collection.db = self
        self.registerSerializers(OT.self, for: collection)
        self.registerMetaSerializers(MT.self, for: collection)
        return collection
    }
}

//MARK: - Serializers
private extension CornucopiaDB.Database {

    class func codableSerializer<T: Codable>(_ type: T.Type, compressionAlgorithm: NSData.CompressionAlgorithm? = nil) -> (String, String, Any) -> Data {

        let serializer = { (collection: String, key: String, object: Any) -> Data in
            guard let o = object as? T else {
                return Data()
            }
            let encoder = MessagePackEncoder()
            do {
                let data = try encoder.encode(o)
                if compressionAlgorithm == nil {
                    return data
                } else {
                    let compressed = try (data as NSData).compressed(using: compressionAlgorithm!) as Data
                    return compressed
                }
            } catch {
                fatalError("Can't encode object: \(error)")
            }
        }
        return serializer
    }

    class func codableDeserializer<T: Codable>(_ type: T.Type, compressionAlgorithm: NSData.CompressionAlgorithm? = nil) -> (String, String, Data) -> T? {

        let deserializer = { (collection: String, key: String, data: Data) -> T? in
            let decoder = MessagePackDecoder()
            do {
                let uncompressed = compressionAlgorithm != nil ? try (data as NSData).decompressed(using: compressionAlgorithm!) as Data: data
                let object = try decoder.decode(T.self, from: uncompressed)
                return object
            } catch {
                fatalError("Can't decode object: \(error)") // if this fails here, you might have changed the struct in a non-compatible way, i.e. by adding a new property or by changing the type of an existing property
            }
        }
        return deserializer
    }

    func registerSerializers<OT: Codable, MT>(_ type: OT.Type, for collection: CornucopiaDB.CodableCollection<OT, MT>) {

        let serializer = Self.codableSerializer(type, compressionAlgorithm: collection.itemCompression)
        self.db.registerSerializer(serializer, forCollection: collection.name)
        let deserializer = Self.codableDeserializer(type, compressionAlgorithm: collection.itemCompression)
        self.db.registerDeserializer(deserializer, forCollection: collection.name)
    }

    func registerMetaSerializers<OT: Codable, MT>(_ type: MT.Type, for collection: CornucopiaDB.CodableCollection<OT, MT>) where MT: Codable {

        let serializer = Self.codableSerializer(type, compressionAlgorithm: collection.metaCompression)
        self.db.registerMetadataSerializer(serializer, forCollection: collection.name)
        let deserializer = Self.codableDeserializer(type, compressionAlgorithm: collection.metaCompression)
        self.db.registerMetadataDeserializer(deserializer, forCollection: collection.name)
    }
}
