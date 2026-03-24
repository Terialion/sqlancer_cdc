# GitHub Upload Bundle

This folder is prepared for direct upload to a GitHub repository.

## Included Scope
- Playbook document
- End-to-end runner
- Sink profile abstraction
- Pipeline definitions
- SQL generators
- CDC validation and test scripts

## Suggested Git Commands

```bash
# From repository root
cd /home/wyh/flink-cdc/tools/cdcup

git add playbook_bundle/github_upload_bundle
git commit -m "chore: add github upload bundle for cdc playbook and scripts"
git push origin <your-branch>
```

## Notes
- Review files before commit to avoid pushing secrets.
- Keep single file size below 100MB for standard GitHub push.
- Prefer PR workflow if default branch is protected.
