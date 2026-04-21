# Blue-Green Stable Switch Layer

These manifests create a stable traffic entrypoint that targets one active Helm
release at a time. Two Helm releases run side-by-side (`durable-engine-blue`
and `durable-engine-green`); a stable `Service` selects whichever one is
currently promoted.

## Values file layering

Production values are composed from three files that separate concerns:

1. `values-production.yaml` — base production config (replicas, resources, sinks).
2. `values-production-<color>.yaml` — color identity only (image tag, engine
   name). Two files: `-blue.yaml` and `-green.yaml`.
3. `values-production-standby.yaml` — role overrides for the inactive color.
   Disables ingestion and forces all sinks into `mode: "mock"`.

Layer them according to role:

- **Active release**: `-f values-production.yaml -f values-production-<color>.yaml`
- **Standby release**: `-f values-production.yaml -f values-production-<color>.yaml -f values-production-standby.yaml`

This keeps color a pure "slot" and role (active vs. standby) orthogonal.

## Resources

- `stable-service.yaml`
  - Service name: `durable-engine-stable`
  - Selects pods by:
    - `app.kubernetes.io/name: durable-engine`
    - `app.kubernetes.io/instance: durable-engine-blue` (default active color)
- `stable-ingress.yaml`
  - Templated with `${BG_HOST}` and `${BG_INGRESS_CLASS}` — rendered via `envsubst`.
  - Routes `/ingest` and `/health` to `durable-engine-stable`.

## Apply stable switch layer

```bash
make k8s-apply-stable-switch BG_HOST=durable-engine.yourdomain.com BG_INGRESS_CLASS=nginx
```

`envsubst` (from `gettext-base`) must be installed.

## Deploy colors

Initial bring-up — deploy one color active, the other standby:

```bash
make k8s-deploy-blue          # active
make k8s-deploy-green-standby # standby
```

Override image tag:

```bash
make k8s-deploy-blue BG_IMAGE_TAG=<image-tag>
make k8s-deploy-green-standby BG_IMAGE_TAG=<image-tag>
```

## Cutover

1. **Deploy new version to the inactive release as a warm preflight**, i.e. as
   active (not standby) so it is ready to take traffic instantly:

   ```bash
   make k8s-deploy-green BG_IMAGE_TAG=<new-tag>   # if green was inactive
   ```

   Between this step and the revert step below, both colors briefly consume
   the live source — accept this duplicate window, or convert to a streaming
   offset-handoff design (see note below).

2. **Switch the stable selector**:

   ```bash
   make k8s-switch-active-color ACTIVE_COLOR=green
   ```

3. **Revert the former-active to standby** so it stops consuming:

   ```bash
   make k8s-revert-to-standby
   ```

### Rollback

Flip the selector back:

```bash
make k8s-rollback-switch
```

(This does not revert standby config; run `k8s-revert-to-standby` again after.)

## Verify

```bash
make k8s-show-active-status
make k8s-verify-persistence-isolation
```

`verify-persistence-isolation` checks that blue and green use disjoint PVC
names and each is `ReadWriteOnce`.

## Streaming offset handoff (known limitation)

With ingestion disabled on the standby color, switching to it means the newly
promoted pods start from `auto_offset_reset: latest` (or from whatever local
checkpoint file exists on that color's PVC, which is empty on first promotion).
For Kafka / CDC consumers this can mean a gap at cutover or duplicates during
the warm-preflight window above. A true zero-gap handoff requires both pods
sharing a consumer group (letting Kafka rebalance) and a shared RWX checkpoint
volume — not modelled here.
