git add .
git commit -m "add files"
export AZUREOIDC_DRIVERS_TAR_FILE=/tmp/mongo-rust-driver.tgz
git archive -o $AZUREOIDC_DRIVERS_TAR_FILE HEAD
export AZUREOIDC_TEST_CMD="PROJECT_DIRECTORY='.' ./.evergreen/install-dependencies.sh rust\
                           && PROJECT_DIRECTORY='.' .evergreen/install-dependencies.sh junit-dependencies\
                           && PROJECT_DIRECTORY='.' OIDC_ENV=azure OIDC=oidc  ./.evergreen/run-mongodb-oidc-test.sh"
bash $DRIVERS_TOOLS/.evergreen/auth_oidc/azure/run-driver-test.sh
