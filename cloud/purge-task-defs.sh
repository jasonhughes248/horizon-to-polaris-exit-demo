# purge-task-defs.sh: sourced by deploy-polaris.sh and teardown.sh.
#
# purge_task_defs ARN...: deregister (if still ACTIVE) and permanently delete ECS
# task definition revisions. DeregisterTaskDefinition alone leaves the revision
# INACTIVE but still describable, plaintext env included. DeleteTaskDefinitions
# accepts at most 10 ARNs per call; revisions already deleted are tolerated.
purge_task_defs() {
  [[ $# -gt 0 ]] || return 0
  local TD BATCH=() FAILED
  for TD in "$@"; do
    aws ecs deregister-task-definition --task-definition "$TD" --region "$AWS_REGION" >/dev/null 2>&1 || true
  done
  _delete_batch() {
    [[ $# -gt 0 ]] || return 0
    FAILED=$(aws ecs delete-task-definitions --task-definitions "$@" --region "$AWS_REGION" \
      --query "failures[?reason!='The specified task definition does not exist.'].[arn,reason]" --output text)
    if [[ -n "$FAILED" && "$FAILED" != "None" ]]; then
      echo "Warning: could not delete task definitions: $FAILED" >&2
    fi
  }
  for TD in "$@"; do
    BATCH+=("$TD")
    if [[ ${#BATCH[@]} -eq 10 ]]; then _delete_batch "${BATCH[@]}"; BATCH=(); fi
  done
  _delete_batch ${BATCH[@]+"${BATCH[@]}"}
  echo "[$(date '+%H:%M:%S')] Deleted $# old task definition revision(s)"
}
