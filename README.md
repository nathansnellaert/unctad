# unctad

This connector is currently in development. It is published publicly on GitHub to leverage Actions compute. When it reaches a more mature stage, the repository will be recreated and a proper README added.

## Memory Management

This connector processes datasets in isolated subprocesses with memory limits to prevent OOM crashes. 

Set the `MAX_PROCESS_MEMORY` environment variable to control memory usage:
- Default: 5GB per dataset subprocess
- Example: `MAX_PROCESS_MEMORY=2` (limits each dataset to 2GB)
- The main process requires ~1GB additional memory

On GitHub Actions with 6GB total memory, the default 5GB limit leaves 1GB for the main process.