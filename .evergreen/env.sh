set -e
set -x

export BASEDIR="$PWD/.evergreen"
export PATH="$BASEDIR/mingit/cmd:$BASEDIR/mingit/mingw64/libexec/git-core:$BASEDIR/git-2:$BASEDIR/node-v$NODE_JS_VERSION-win-x64:/opt/python/3.6/bin:/opt/chefdk/gitbin:/cygdrive/c/Python310/Scripts:/cygdrive/c/Python310:/cygdrive/c/cmake/bin:/opt/mongodbtoolchain/v3/bin:$PATH"

. ~/.cargo/env

if [ "$OS" != "Windows_NT" ]; then
  if which realpath; then # No realpath on macOS, but also not needed there
    export HOME="$(realpath "$HOME")" # Needed to de-confuse nvm when /home is a symlink
  fi
  export NVM_DIR="$HOME/.nvm"
  echo "Setting NVM environment home: $NVM_DIR"
  [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
  export PATH="$NVM_BIN:$PATH"

  if [ -x "$BASEDIR/git-2/git" ]; then
    export GIT_EXEC_PATH="$BASEDIR/git-2"
  fi
fi
