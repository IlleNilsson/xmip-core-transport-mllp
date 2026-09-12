# xmip-core-transport-mllp

HL7 Minimal Lower Layer Protocol over TCP: a framed message is one Stream, and the acknowledgment travels back on the same connection. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
