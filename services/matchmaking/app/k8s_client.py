import logging
import os

logger = logging.getLogger("matchmaking-k8s")

_current_target_replicas = None
_STATEFULSET_NAME = "game-room"
_MAX_REPLICAS = 10


async def ensure_game_rooms(desired_count: int) -> int | None:
    """Scale game-room StatefulSet so replicas >= desired_count.

    Returns the new replica count, or None in non-K8s environments.
    Caps at _MAX_REPLICAS to respect HPA limits.
    """
    global _current_target_replicas
    try:
        from kubernetes import config, client

        config.load_incluster_config()
        api = client.AppsV1Api()

        try:
            with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
                ns = f.read().strip()
        except Exception:
            ns = "game-platform"

        statefulset = api.read_namespaced_stateful_set(_STATEFULSET_NAME, ns)
        current = statefulset.spec.replicas

        if current >= desired_count:
            logger.info(f"game-room already has {current} replicas (needed {desired_count})")
            return current

        new_replicas = min(desired_count, _MAX_REPLICAS)
        if new_replicas == _current_target_replicas:
            logger.info(f"Scale to {new_replicas} already in-flight, skipping")
            return current

        body = {"spec": {"replicas": new_replicas}}
        api.patch_namespaced_stateful_set(_STATEFULSET_NAME, ns, body)
        _current_target_replicas = new_replicas
        logger.info(f"Scaled {_STATEFULSET_NAME} from {current} to {new_replicas}")
        return new_replicas
    except Exception as e:
        logger.warning(f"K8s scale-up unavailable (non-K8s env?): {e}")
        return None
