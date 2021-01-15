//
//  Cornucopia – (C) Dr. Lauer Information Technology
//
import YapDatabase

public extension CornucopiaDB {

    class ViewMappings {

        let dbViewMappings: YapDatabaseViewMappings

        init<OT, MT>(view: View<OT, MT>) {

            let groupFiltering: YapDatabaseViewMappingGroupFilter = { (group, transaction) in
                true
            }
            let groupSorting: YapDatabaseViewMappingGroupSort = { (group1, group2, transaction) in
                group1.compare(group2)
            }
            self.dbViewMappings = YapDatabaseViewMappings(groupFilterBlock: groupFiltering, sortBlock: groupSorting, view: view.name)
        }

    }

}
