//
//  Cornucopia – (C) Dr. Lauer Information Technology
//
import YapDatabase

import os.log // would really like to use OSLog here, but for now let's retain compatibility to iOS 13
private let log = OSLog(subsystem: "Cornucopia.DB", category: "AutoView")

public extension CornucopiaDB {

    class AutoView<OT: Codable, MT: Any>: View<OT, MT> {

        public typealias Transaction = YapDatabaseAutoViewTransaction

        public typealias GenericKeyToString = (String) -> String?
        public typealias GenericObjectToString<OT> = (OT) -> String?
        public typealias GenericKeyObjectToString<OT> = (String, OT) -> String?
        public typealias GenericKeyMetaToString<MT> = (String, MT) -> String?
        public typealias GenericRowToString<OT, MT> = (String, OT, MT) -> String?

        public enum Grouping {
            // standard cases for views based on one – whitelisted – collection
            case single(name: String = "default") // this is merely syntactic sugar for `.byKey` with a closure returning a constant string.
            case byKey(closure: GenericKeyToString)
            case byObject(closure: GenericObjectToString<OT>)
            case byKeyObject(closure: GenericKeyObjectToString<OT>)
            case byKeyMeta(closure: GenericKeyMetaToString<MT>)
            case byRow(closure: GenericRowToString<OT, MT>)

            var grouping: YapDatabaseViewGrouping {
                switch self {

                    case .single(name: let name):
                        let grouping = YapDatabaseViewGrouping.withKeyBlock { (_, _, _) -> String? in
                            return name
                        }
                        return grouping

                    case .byKey(closure: let closure):
                        let grouping = YapDatabaseViewGrouping.withKeyBlock { (_, _, key) -> String? in
                            return closure(key)
                        }
                        return grouping

                    case .byObject(closure: let closure):
                        let grouping = YapDatabaseViewGrouping.withObjectBlock { (_, _, _, object: Any) -> String? in
                            guard let object = object as? OT else { return nil }
                            return closure(object)
                        }
                        return grouping

                    case .byKeyObject(closure: let closure):
                        let grouping = YapDatabaseViewGrouping.withObjectBlock { (_, _, key, object: Any) -> String? in
                            guard let object = object as? OT else { return nil }
                            return closure(key, object)
                        }
                        return grouping

                    case .byKeyMeta(closure: let closure):
                        let grouping = YapDatabaseViewGrouping.withMetadataBlock { (_, _, key, meta: Any) -> String? in
                            guard let meta = meta as? MT else { return nil }
                            return closure(key, meta)
                        }
                        return grouping

                    case .byRow(closure: let closure):
                        let grouping = YapDatabaseViewGrouping.withRowBlock { (_, _, key, object: Any, meta: Any) -> String? in
                            guard let object = object as? OT else { return nil }
                            guard let meta = meta as? MT else { return nil }
                            return closure(key, object, meta)
                        }
                        return grouping
                }
            }
        }

        public typealias GenericKeyToComparison = (String, String) -> ComparisonResult
        public typealias GenericObjectToComparison<OT> = (OT, OT) -> ComparisonResult
        public typealias GenericKeyObjectToComparison<OT> = (String, OT, String, OT) -> ComparisonResult
        public typealias GenericKeyMetaToComparison<MT> = (String, MT, String, MT) -> ComparisonResult
        public typealias GenericRowToComparison<OT, MT> = (String, OT, MT, String, OT, MT) -> ComparisonResult

        public enum ViewSortFunction {
            case byKey(closure: GenericKeyToComparison)
            case byObject(closure: GenericObjectToComparison<OT>)
            case byComparing(keyPath: (OT) -> (String), options: String.CompareOptions = .caseInsensitive)
            case byKeyObject(closure: GenericKeyObjectToComparison<OT>)
            case byKeyMeta(closure: GenericKeyMetaToComparison<MT>)
            case byRow(closure: GenericRowToComparison<OT, MT>)

            var sorting: YapDatabaseViewSorting {
                switch self {

                    case .byComparing(keyPath: let keypath, options: let options):
                        let sorting = YapDatabaseViewSorting.withObjectBlock { (_, _, _, _, object1: Any, _, _, object2: Any) -> ComparisonResult in
                            guard let object1 = object1 as? OT else { fatalError("Internal error") }
                            guard let object2 = object2 as? OT else { fatalError("Internal error") }
                            let value1 = keypath(object1)
                            let value2 = keypath(object2)
                            return value1.compare(value2, options: options)
                        }
                        return sorting

                    case .byKey(closure: let closure):
                        let sorting = YapDatabaseViewSorting.withKeyBlock { (_, _, _, key1, _, key2) -> ComparisonResult in
                            return closure(key1, key2)
                        }
                        return sorting

                    case .byObject(closure: let closure):
                        let sorting = YapDatabaseViewSorting.withObjectBlock { (_, _, _, _, object1: Any, _, _, object2: Any) -> ComparisonResult in
                            guard let object1 = object1 as? OT else { fatalError("Internal error") }
                            guard let object2 = object2 as? OT else { fatalError("Internal error") }
                            return closure(object1, object2)
                        }
                        return sorting

                    case .byKeyObject(closure: let closure):
                        let sorting = YapDatabaseViewSorting.withObjectBlock { (_, _, _, key1, object1: Any, _, key2, object2: Any) -> ComparisonResult in
                            guard let object1 = object1 as? OT else { fatalError("Internal error") }
                            guard let object2 = object2 as? OT else { fatalError("Internal error") }
                            return closure(key1, object1, key2, object2)
                        }
                        return sorting

                    case .byKeyMeta(closure: let closure):
                        let sorting = YapDatabaseViewSorting.withMetadataBlock { (_, _, _, key1, meta1: Any, _, key2, meta2: Any) -> ComparisonResult in
                            guard let meta1 = meta1 as? MT else { fatalError("Internal error") }
                            guard let meta2 = meta2 as? MT else { fatalError("Internal error") }
                            return closure(key1, meta1, key2, meta2)
                        }
                        return sorting

                    case .byRow(closure: let closure):
                        let sorting = YapDatabaseViewSorting.withRowBlock { (_, _, _, key1, object1: Any, meta1: Any, _, key2, object2: Any, meta2: Any) -> ComparisonResult in
                            guard let object1 = object1 as? OT else { fatalError("Internal error") }
                            guard let meta1 = meta1 as? MT else { fatalError("Internal error") }
                            guard let object2 = object2 as? OT else { fatalError("Internal error") }
                            guard let meta2 = meta2 as? MT else { fatalError("Internal error") }
                            return closure(key1, object1, meta1, key2, object2, meta2)
                        }
                        return sorting
                }
            }
        }

        internal init(name: String = String(describing: type(of: OT.self)),
                    versionTag: String? = nil,
                    persistent: Bool = true,
                    allowedCollections: Set<String> = Set(),
                    grouping: Grouping,
                    sorting: ViewSortFunction) {

            let grouping = grouping.grouping
            let sorting = sorting.sorting

            let options: YapDatabaseViewOptions? = {
                guard !persistent || !allowedCollections.isEmpty else { return nil }
                let yco = YapDatabaseViewOptions()
                yco.isPersistent = persistent
                if !allowedCollections.isEmpty {
                    yco.allowedCollections = YapWhitelistBlacklist(whitelist: allowedCollections)
                }
                return yco
            }()

            let dbView = YapDatabaseAutoView(grouping: grouping, sorting: sorting, versionTag: versionTag, options: options)
            super.init(name: name, dbView: dbView)
        }
    }
}
