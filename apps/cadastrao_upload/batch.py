import io
import json
from datetime import datetime, timezone
from pathlib import PurePosixPath
from uuid import uuid4

EXCEL_SUFFIXES = {".xlsx", ".xls", ".xlsm"}
MAX_FILE_BYTES = 50 * 1024**2
MAX_BATCH_BYTES = 200 * 1024**2


def prepare_files(selected):
    if not selected:
        raise ValueError("Selecione pelo menos uma planilha.")

    files = []
    names = set()
    total_size = 0
    for file in selected:
        name = file.name
        if (
            not name
            or name != name.strip()
            or "/" in name
            or chr(92) in name
            or not name.isprintable()
            or PurePosixPath(name).suffix.lower() not in EXCEL_SUFFIXES
        ):
            raise ValueError("Use planilhas Excel com nomes válidos e sem caminhos.")
        if name.casefold() in names:
            raise ValueError(f"Nome repetido no lote: {name}")
        if not 0 < file.size <= MAX_FILE_BYTES:
            raise ValueError(f"O arquivo {name} está vazio ou excede o limite por arquivo.")
        names.add(name.casefold())
        total_size += file.size
        files.append(file)

    if total_size > MAX_BATCH_BYTES:
        raise ValueError("O lote excede o limite total de 200 MB.")
    return files


def upload_batch(storage, root, user, files):
    root = root.rstrip("/")
    parts = root.split("/")
    if (
        not root.startswith("/Volumes/")
        or len(parts) < 5
        or any(part in {"", ".", ".."} for part in parts[2:])
    ):
        raise ValueError("Configure CADASTRAO_INPUT_PATH com um caminho /Volumes válido.")

    batch_id = uuid4().hex
    batch_path = f"{root}/{batch_id}"
    storage.create_directory(batch_path)

    for file in files:
        file.seek(0)
        storage.upload(f"{batch_path}/{file.name}", file, overwrite=False)

    metadata = {
        "batch_id": batch_id,
        "uploaded_by": user,
        "upload_completed_at": datetime.now(timezone.utc).isoformat(),
        "files": [{"name": file.name, "size_bytes": file.size} for file in files],
    }
    storage.upload(
        f"{batch_path}/upload_metadata.json",
        io.BytesIO(json.dumps(metadata, ensure_ascii=False).encode("utf-8")),
        overwrite=False,
    )

    manifest = "".join(f"{file.name}\n" for file in files)
    storage.upload(
        f"{batch_path}/lote_pronto.txt",
        io.BytesIO(manifest.encode("utf-8")),
        overwrite=False,
    )
    return batch_id
