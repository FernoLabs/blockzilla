# Edgezilla

These Workers are built and deployed separately from the NAS services.

- [get-block](get-block/README.md): Archive V2 edge API.
- [of-get-block](of-get-block/README.md): Old Faithful compatibility API.
- [archive-samples](archive-samples/README.md): read-only CAR, V2, and V3 sample gateway.
- [r2-gateway](r2-gateway/README.md): archive object gateway.

Each Worker keeps its own build and deployment configuration. The folder move
does not change deployed names, routes, or object keys.
