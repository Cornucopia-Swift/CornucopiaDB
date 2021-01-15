//
//  Cornucopia – (C) Dr. Lauer Information Technology
//
import YapDatabase
import Foundation

public struct YapDatabaseViewChanges {

    public let sections: [YapDatabaseViewSectionChange]
    public let rows: [YapDatabaseViewRowChange]
    public var isEmpty: Bool { sections.isEmpty && rows.isEmpty }

}

public extension YapDatabaseViewConnection {

    func changes(for notifications: [Notification], with mappings: YapDatabaseViewMappings) -> YapDatabaseViewChanges {

        var sectionChangesArray = NSArray()
        var rowChangesArray = NSArray()
        self.__getSectionChanges(&sectionChangesArray, rowChanges: &rowChangesArray, for: notifications, with: mappings)
        return YapDatabaseViewChanges(sections: sectionChangesArray as! [YapDatabaseViewSectionChange], rows: rowChangesArray as! [YapDatabaseViewRowChange])
    }

}
