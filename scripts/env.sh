# Shared settings for the scripts. Sourced, not run; the caller sets ROOT.

EXE=""
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*)
    EXE=".exe"
    # Go and protoc from MSYS2 (pacman -S mingw-w64-x86_64-go mingw-w64-x86_64-protobuf).
    if [ -d /c/msys64/mingw64/bin ]; then
      export PATH="/c/msys64/mingw64/bin:$PATH"
      [ -d /c/msys64/mingw64/lib/go ] && export GOROOT="${GOROOT:-C:/msys64/mingw64/lib/go}"
    fi
    ;;
esac
export PATH="$(go env GOPATH 2> /dev/null | tr '\\' '/' | sed 's#^\([A-Za-z]\):#/\L\1#')/bin:$PATH"

# Output outside the repo when it sits in OneDrive (OneDrive locks synced build folders).
LEDGERLINE_DEFAULT_OUT="$ROOT/.out"
case "$ROOT" in
  */OneDrive*) LEDGERLINE_DEFAULT_OUT="$(cygpath -u "$LOCALAPPDATA")/ledgerline" ;;
esac
LEDGERLINE_OUT="${LEDGERLINE_OUT:-$LEDGERLINE_DEFAULT_OUT}"

# The throwaway PostgreSQL cluster for tests: port 54331, user ledgerline, no password.
LEDGERLINE_PG_PORT="${LEDGERLINE_PG_PORT:-54331}"
LEDGERLINE_PG_DATA="$LEDGERLINE_OUT/pgdata"
LEDGERLINE_LOCAL_DB="postgres://ledgerline@localhost:$LEDGERLINE_PG_PORT/postgres?sslmode=disable"

# The pinned protoc release, so generated code is byte-identical everywhere (CI checks it).
PROTOC_VERSION="33.5"
