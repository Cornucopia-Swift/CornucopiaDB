//
//  Cornucopia – (C) Dr. Lauer Information Technology
//
import YapDatabase

import os.log // would really like to use OSLog here, but for now let's retain compatibility to iOS 13
private let log = OSLog(subsystem: "Cornucopia.DB", category: "ManualView")

public extension CornucopiaDB {

    class ManualView<OT: Codable, MT: Any>: View<OT, MT> {

        internal init(name: String = String(describing: type(of: OT.self)),
                      versionTag: String? = nil,
                      persistent: Bool = true,
                      allowedCollections: Set<String> = Set()) {

            let options: YapDatabaseViewOptions? = {
                guard !persistent || !allowedCollections.isEmpty else { return nil }
                let yco = YapDatabaseViewOptions()
                yco.isPersistent = persistent
                if !allowedCollections.isEmpty {
                    yco.allowedCollections = YapWhitelistBlacklist(whitelist: allowedCollections)
                }
                return yco
            }()

            let dbView = YapDatabaseManualView(versionTag: versionTag, options: options)
            super.init(name: name, dbView: dbView)
        }
    }
}

public extension CornucopiaDB.CodableCollection {

    func isItem(_ item: OT,
                 /* in group: String = "default", */
                 in manualView: CornucopiaDB.ManualView<OT, MT>,
                 via transaction: YapDatabaseReadTransaction? = nil,
                 on connection: YapDatabaseConnection? = nil) -> Bool {

        precondition(db != nil, "Collection is not registered with a database yet")
        precondition((transaction != nil && connection == nil) ||
                        (connection != nil && transaction == nil) ||
                        (transaction == nil && connection == nil), "Unsupported combination of non-nil transaction and non-nil connection")

        var result = false

        let block: (YapDatabaseReadTransaction) -> (Void) = { t in
            guard let vt = t.extension(manualView.name) as? YapDatabaseManualViewTransaction else {
                fatalError("Could not get manual view transaction for '\(manualView.name)'")
            }
            let k = self.keyFunction(item)
            result = vt.group(forKey: k, inCollection: self.name) != nil
        }

        guard let t = transaction else {
            let connection = connection ?? self.db.defaultConnection
            connection.read(block)
            return result
        }
        block(t)
        return result
    }

    func addItem(_ item: OT,
                 for group: String = "default",
                 in manualView: CornucopiaDB.ManualView<OT, MT>,
                 via transaction: YapDatabaseReadWriteTransaction? = nil,
                 on connection: YapDatabaseConnection? = nil) {

        precondition(db != nil, "Collection is not registered with a database yet")
        precondition((transaction != nil && connection == nil) ||
                        (connection != nil && transaction == nil) ||
                        (transaction == nil && connection == nil), "Unsupported combination of non-nil transaction and non-nil connection")

        let block: (YapDatabaseReadWriteTransaction) -> () = { t in
            guard let vt = t.extension(manualView.name) as? YapDatabaseManualViewTransaction else {
                fatalError("Could not get manual view transaction for '\(manualView.name)'")
            }
            let k = self.keyFunction(item)
            vt.addKey(k, inCollection: self.name, toGroup: group)
        }

        guard let t = transaction else {
            let connection = connection ?? self.db.defaultConnection
            connection.readWrite(block)
            return
        }
        block(t)
    }

    func insertItem(_ item: OT,
                    at index: Int,
                    for group: String = "default",
                    in manualView: CornucopiaDB.ManualView<OT, MT>,
                    via transaction: YapDatabaseReadWriteTransaction? = nil,
                    on connection: YapDatabaseConnection? = nil) {

        precondition(db != nil, "Collection is not registered with a database yet")
        precondition((transaction != nil && connection == nil) ||
                        (connection != nil && transaction == nil) ||
                        (transaction == nil && connection == nil), "Unsupported combination of non-nil transaction and non-nil connection")

        let block: (YapDatabaseReadWriteTransaction) -> () = { t in
            guard let vt = t.extension(manualView.name) as? YapDatabaseManualViewTransaction else {
                fatalError("Could not get manual view transaction for '\(manualView.name)'")
            }
            let k = self.keyFunction(item)
            vt.insertKey(k, inCollection: self.name, at: UInt(index), inGroup: group)
        }

        guard let t = transaction else {
            let connection = connection ?? self.db.defaultConnection
            connection.readWrite(block)
            return
        }
        block(t)
    }

    func removeItem(_ item: OT,
                    from group: String = "default",
                    in manualView: CornucopiaDB.ManualView<OT, MT>,
                    via transaction: YapDatabaseReadWriteTransaction? = nil,
                    on connection: YapDatabaseConnection? = nil) {

        precondition(db != nil, "Collection is not registered with a database yet")
        precondition((transaction != nil && connection == nil) ||
                        (connection != nil && transaction == nil) ||
                        (transaction == nil && connection == nil), "Unsupported combination of non-nil transaction and non-nil connection")

        let block: (YapDatabaseReadWriteTransaction) -> () = { t in
            guard let vt = t.extension(manualView.name) as? YapDatabaseManualViewTransaction else {
                fatalError("Could not get manual view transaction for '\(manualView.name)'")
            }
            let k = self.keyFunction(item)
            vt.removeKey(k, inCollection: self.name, fromGroup: group)
        }

        guard let t = transaction else {
            let connection = connection ?? self.db.defaultConnection
            connection.readWrite(block)
            return
        }
        block(t)
    }

    func removeItem(at index: Int,
                    from group: String = "default",
                    in manualView: CornucopiaDB.ManualView<OT, MT>,
                    via transaction: YapDatabaseReadWriteTransaction? = nil,
                    on connection: YapDatabaseConnection? = nil) {

        precondition(db != nil, "Collection is not registered with a database yet")
        precondition((transaction != nil && connection == nil) ||
                        (connection != nil && transaction == nil) ||
                        (transaction == nil && connection == nil), "Unsupported combination of non-nil transaction and non-nil connection")

        let block: (YapDatabaseReadWriteTransaction) -> () = { t in
            guard let vt = t.extension(manualView.name) as? YapDatabaseManualViewTransaction else {
                fatalError("Could not get manual view transaction for '\(manualView.name)'")
            }
            vt.removeItem(at: UInt(index), inGroup: group)
        }

        guard let t = transaction else {
            let connection = connection ?? self.db.defaultConnection
            connection.readWrite(block)
            return
        }
        block(t)
    }
}
