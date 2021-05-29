import XCTest
@testable import CornucopiaDB

import Foundation

final class CornucopiaDBTests: XCTestCase {

    func testCreate() {

        let db = CornucopiaDB.Database(name: "\(UUID())")
        XCTAssertEqual(true, true)
    }

    func testCodableCollection() {

        struct CodableStruct: Codable, Equatable {
            let key: String
            let i: Int
            let s: String
            let d: Date
        }

        let db = CornucopiaDB.Database(name: "\(UUID())")
        let coll = db.codableCollection(keyed: \CodableStruct.key)
        let item = CodableStruct(key: "foo", i: 42, s: "bar", d: Date())
        coll.persist(item: item)

        let decodedItem = coll.item(for: "foo")
        XCTAssertEqual(decodedItem, item)






        

    }

    static var allTests = [
        ("testCreate", testCreate),
        ("testCodableCollection", testCodableCollection),
    ]
}
