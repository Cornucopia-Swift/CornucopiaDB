//
//  Cornucopia – (C) Dr. Lauer Information Technology
//
import YapDatabase

public protocol _NamedViewExtension {

    associatedtype TransactionType

    var name: String { get }
    func viewTransaction(for transaction: YapDatabaseReadTransaction) -> TransactionType
    func viewTransaction(for transaction: YapDatabaseReadWriteTransaction) -> TransactionType
}

extension _NamedViewExtension {

    public func viewTransaction(for transaction: YapDatabaseReadTransaction) -> TransactionType {
        guard let vt = transaction.extension(self.name) as? TransactionType else { fatalError("Can't find view transaction for \(self.name)")}
        return vt
    }

    public func viewTransaction(for transaction: YapDatabaseReadWriteTransaction) -> TransactionType {
        guard let vt = transaction.extension(self.name) as? TransactionType else { fatalError("Can't find view transaction for \(self.name)")}
        return vt
    }
}

public extension CornucopiaDB {
    typealias NamedViewExtension = _NamedViewExtension
}

public extension CornucopiaDB {

    class View<OT: Codable, MT>: NamedViewExtension {

        public typealias TransactionType = YapDatabaseViewTransaction

        public let name: String
        let dbView: YapDatabaseView
        var database: Database!

        internal init(name: String, dbView: YapDatabaseView) {
            self.name = name
            self.dbView = dbView
        }

        /// Returns a child view applying `filtering` to all the elements in this view.
        /// NOTE: This blocks the calling thread.
        public func filtered(name: String = String(describing: OT.self) + ".filtered",
                             persistent: Bool = true,
                             versionTag: String? = nil,
                             filtering: CornucopiaDB.FilteredView<OT, MT>.FilterFunction) -> CornucopiaDB.FilteredView<OT, MT> {
            precondition(self.database != nil, "View '\(self.name)' is not registered yet")

            let filteredView = CornucopiaDB.FilteredView<OT, MT>(name: name, sourceView: self, persistent: persistent, versionTag: versionTag, filtering: filtering)
            self.database.register(view: filteredView)
            return filteredView
        }

        /// Creates a child view applying `filtering` to all the elements in this view.
        /// NOTE: Work is being done on a secondary queue.
        public func filtered(name: String = String(describing: OT.self) + ".filtered",
                             persistent: Bool = true,
                             versionTag: String? = nil,
                             filtering: CornucopiaDB.FilteredView<OT, MT>.FilterFunction,
                             then: @escaping( (Bool) -> Void)) {
            precondition(self.database != nil, "View '\(self.name)' is not registered yet")

            let filteredView = CornucopiaDB.FilteredView<OT, MT>(name: name, sourceView: self, persistent: persistent, versionTag: versionTag, filtering: filtering)
            self.database.register(view: filteredView, then: then)
        }
    }
}
