//
//  Cornucopia – (C) Dr. Lauer Information Technology
//
import YapDatabase

public extension CornucopiaDB {

    ///TBD
    class MultiCollection<OT: Codable, MT> {

        let name: String
        weak var db: Database!

        internal init(name: String, db: Database) {
            self.name = name
            self.db = db
        }

        /// Returns a view configured for the elements in all collections that match the generic type specification
        public func typedView(name: String? = nil,
                       versionTag: String? = nil,
                       persistent: Bool = true,
                       /* filter? */
                       grouping: CornucopiaDB.AutoView<OT, MT>.Grouping,
                       sorting: CornucopiaDB.AutoView<OT, MT>.ViewSortFunction) -> CornucopiaDB.AutoView<OT, MT> {

            precondition(self.db != nil, "Collection is not registered with a database yet")

            let view = CornucopiaDB.AutoView<OT, MT>(name: name ?? "\(self).typedView",
                                                     versionTag: versionTag,
                                                     persistent: persistent,
                                                     grouping: grouping,
                                                     sorting: sorting)

            self.db.register(view: view)
            return view
        }
    }
}
