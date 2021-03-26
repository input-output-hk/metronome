#!/usr/bin/env bash

set -euv

echo $GPG_KEY | base64 --decode | gpg --batch --import

gpg --passphrase $GPG_PASSPHRASE --batch --yes -a -b LICENSE

if [[ "$CIRCLE_BRANCH" == "develop" ]]; then

mill mill.scalalib.PublishModule/publishAll \
    __.publishArtifacts \
    "$OSS_USERNAME":"$OSS_PASSWORD" \
    --gpgArgs --passphrase="$GPG_PASSPHRASE",--batch,--yes,-a,-b

elif [[ "$CIRCLE_BRANCH" == "master" ]]; then

mill versionFile.setReleaseVersion
mill mill.scalalib.PublishModule/publishAll \
    __.publishArtifacts \
    "$OSS_USERNAME":"$OSS_PASSWORD" \
    --gpgArgs --passphrase="$GPG_PASSPHRASE",--batch,--yes,-a,-b \
    --readTimeout 600000 \
    --awaitTimeout 600000 \
    --release true

else

  echo "Skipping publish step"

fi
