//
//  Cornucopia – (C) Dr. Lauer Information Technology
//
import YapDatabase

public extension YapDatabaseReadTransaction {

    func enumerateCollectionNames(_ block: (String, inout Bool) -> Void) {

        let enumBlock = { (collection: String, outerStop: UnsafeMutablePointer<ObjCBool>) -> Void in

            var innerStop = false
            block(collection, &innerStop)

            if innerStop {
                outerStop.pointee = true
            }
        }

        self.__enumerateCollections(enumBlock)
    }
}
