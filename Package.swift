// swift-tools-version:5.3
import PackageDescription

let package = Package(
    name: "CornucopiaDB",
    platforms: [
        .iOS(.v13),
        .macOS(.v10_15),
        .tvOS(.v13),
        .watchOS(.v6),
    ],
    products: [
        .library(
            name: "CornucopiaDB",
            targets: ["CornucopiaDB"]),
    ],
    dependencies: [
        .package(name: "Swift-YapDatabase", url: "https://github.com/mickeyl/SwiftYapDatabase", .branch("master")),
        .package(name: "MessagePacker", url: "https://github.com/mickeyl/MessagePacker", .branch("master")),
        .package(name: "ULID.swift", url: "https://github.com/yaslab/ULID.swift", .upToNextMajor(from: "1.1.0")),
    ],
    targets: [
        .target(
            name: "CornucopiaDB",
            dependencies: [
                .product(name: "YapDatabase", package: "Swift-YapDatabase"),
                .product(name: "MessagePacker", package: "MessagePacker"),
                .product(name: "ULID", package: "ULID.swift"),
            ]
        ),
        .testTarget(
            name: "CornucopiaDBTests",
            dependencies: ["CornucopiaDB"]),
    ]
)
