# Template setup

Complete this checklist before implementation:

1. Choose the final repository name and confirm that its boundary belongs in the Xmip architecture.
2. Update the architecture specification and `xmip-architecture.json` together when the new repository changes the architecture baseline.
3. Replace `xmip-template`, its description and repository URL in `Cargo.toml`.
4. Replace the template title and instructions in `README.md`.
5. Complete `ARCHITECTURE.md`: classification, maturity, owning capability, responsibility, public contracts, dependencies and non-responsibilities.
6. Keep the full AGPL-3.0 license in `LICENSE` and `license = "AGPL-3.0-or-later"` in `Cargo.toml`.
7. Add verification that proves the repository's accepted responsibility and contracts.
8. Keep account-wide contribution, security, support, issue and pull-request defaults unless a reviewed repository-specific override is required.
9. Decide explicitly whether automatic verification triggers should be enabled. The template includes manual dispatch only.
10. Remove this setup file after every item is complete.

Template repositories are snapshots. Changes made here after generation do not automatically propagate to repositories already created from it.
