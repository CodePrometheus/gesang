INSERT INTO gesang_resource_bindings (
    id, binding, principal, interface_name, interface_version,
    request_id, request_digest, endpoint, package
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (binding, principal, interface_name, interface_version, request_id)
DO UPDATE SET id = gesang_resource_bindings.id
RETURNING id,
          binding AS "binding: _",
          principal AS "principal: _",
          interface_name,
          interface_version,
          request_id,
          request_digest,
          endpoint AS "endpoint: _",
          package AS "package: _",
          created_at;
