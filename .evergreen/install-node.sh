set -e
set -x

export BASEDIR="$PWD/.evergreen"
export PATH="$BASEDIR/mingit/cmd:$BASEDIR/mingit/mingw64/libexec/git-core:$BASEDIR/git-2:$BASEDIR/node-v$NODE_JS_VERSION-win-x64:/opt/python/3.6/bin:/opt/chefdk/gitbin:/cygdrive/c/Python310/Scripts:/cygdrive/c/Python310:/cygdrive/c/cmake/bin:/opt/mongodbtoolchain/v3/bin:$PATH"

if [ "$OS" == "Windows_NT" ]; then
  powershell "$(cygpath -w "$BASEDIR")"/InstallNode.ps1

  # Explicitly grab a fresh portable Git for Windows build
  curl -L https://github.com/git-for-windows/git/releases/download/v2.32.0.windows.2/MinGit-2.32.0.2-busybox-64-bit.zip -o "$BASEDIR/mingit-2.32.0.zip"
  mkdir "$BASEDIR/mingit"
  unzip "$BASEDIR/mingit-2.32.0.zip" -d "$BASEDIR/mingit"
else
  if which realpath; then # No realpath on macOS, but also not needed there
    export HOME="$(realpath "$HOME")" # Needed to de-confuse nvm when /home is a symlink
  fi
  export NVM_DIR="$HOME/.nvm"

  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.38.0/install.sh | bash

  echo "Setting NVM environment home: $NVM_DIR"
  [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
  [ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"

  nvm install --no-progress $NODE_JS_VERSION
  nvm alias default $NODE_JS_VERSION

  if env PATH="/opt/chefdk/gitbin:$PATH" git --version | grep -q 'git version 1.'; then
    (cd "$BASEDIR" &&
      curl -sSfL https://github.com/git/git/archive/refs/tags/v2.31.1.tar.gz | tar -xvz &&
      mv git-2.31.1 git-2 &&
      cd git-2 &&
      make -j8 NO_EXPAT=1)
  fi

  export PATH="$NVM_BIN:$PATH"
fi
