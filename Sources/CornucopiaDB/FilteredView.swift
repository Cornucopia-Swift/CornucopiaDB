//
//  Cornucopia – (C) Dr. Lauer Information Technology
//
import YapDatabase

public extension CornucopiaDB {

    class FilteredView<OT: Codable, MT>: View<OT, MT> {

        //NOTE: This is - unfortunately - useless here, since subclasses "overriding" protocol typealiases does not work in Swift yet
        public typealias TransactionType = YapDatabaseFilteredViewTransaction

        public typealias GenericKeyToBool = (String) -> Bool
        public typealias GenericObjectToBool<OT> = (OT) -> Bool
        public typealias GenericKeyObjectToBool<OT> = (String, OT) -> Bool
        public typealias GenericKeyMetaToBool<MT> = (String, MT) -> Bool
        public typealias GenericRowToBool<OT, MT> = (String, OT, MT) -> Bool

        public enum FilterFunction {
            case byKey(closure: GenericKeyToBool)
            case byObject(closure: GenericObjectToBool<OT>)
            case byKeyObject(closure: GenericKeyObjectToBool<OT>)
            case byKeyMeta(closure: GenericKeyMetaToBool<MT>)
            case byRow(closure: GenericRowToBool<OT, MT>)

            var filtering: YapDatabaseViewFiltering {
                switch self {

                    case .byKey(closure: let closure):
                        let filtering = YapDatabaseViewFiltering.withKeyBlock { (_, _, _, key) -> Bool in
                            return closure(key)
                        }
                        return filtering

                    case .byObject(closure: let closure):
                        let filtering = YapDatabaseViewFiltering.withObjectBlock { (_, _, _, _, object: Any) -> Bool in
                            guard let object = object as? OT else { fatalError("Internal Error") }
                            return closure(object)
                        }
                        return filtering

                    case .byKeyObject(closure: let closure):
                        let filtering = YapDatabaseViewFiltering.withObjectBlock { (_, _, _, key, object: Any) -> Bool in
                            guard let object = object as? OT else { fatalError("Internal Error") }
                            return closure(key, object)
                        }
                        return filtering

                    case .byKeyMeta(closure: let closure):
                        let filtering = YapDatabaseViewFiltering.withMetadataBlock { (_, _, _, key, meta: Any) -> Bool in
                            guard let meta = meta as? MT else { fatalError("Internal Error") }
                            return closure(key, meta)
                        }
                        return filtering

                    case .byRow(closure: let closure):
                        let filtering = YapDatabaseViewFiltering.withRowBlock { (_, _, _, key, object: Any, meta: Any) -> Bool in
                            guard let object = object as? OT else { fatalError("Internal Error") }
                            guard let meta = meta as? MT else { fatalError("Internal Error") }
                            return closure(key, object, meta)
                        }
                        return filtering
                }
            }
        }

        internal init(name: String, sourceView: CornucopiaDB.View<OT, MT>, persistent: Bool = true, versionTag: String? = nil, filtering: FilterFunction) {

            let options: YapDatabaseViewOptions = {
                let o = YapDatabaseViewOptions()
                o.isPersistent = persistent
                return o
            }()

            let filtering = filtering.filtering
            let dbView = YapDatabaseFilteredView(parentViewName: sourceView.name, filtering: filtering, versionTag: versionTag, options: options)
            super.init(name: name, dbView: dbView)
        }

        public func updateFiltering(via connection: YapDatabaseConnection, filtering: FilterFunction) {
            connection.readWrite { t in
                //FIXME: Overriding typealias does not work, hence we need to explicitly state out transaction type here
                guard let vt = self.viewTransaction(for: t) as? YapDatabaseFilteredViewTransaction else { fatalError() }
                vt.setFiltering(filtering.filtering, versionTag: String("\(Date().timeIntervalSince1970)"))
            }
        }

        public func updateFiltering(via connection: YapDatabaseConnection, filtering: FilterFunction, then: @escaping( ()->() )) {
            connection.asyncReadWrite( { t in
                guard let vt = self.viewTransaction(for: t) as? YapDatabaseFilteredViewTransaction else { fatalError() }
                //FIXME: Overriding typealias does not work, hence we need to explicitly state out transaction type here
                vt.setFiltering(filtering.filtering, versionTag: String("\(Date().timeIntervalSince1970)"))
            }, completionBlock: then)
        }
    }
}
