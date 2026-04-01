"""
Upload service.

Upload service for assets.grok.com.
"""

import base64
import binascii
import hashlib
import mimetypes
import re
import secrets
import struct
import zlib
from pathlib import Path
from typing import AsyncIterator, Optional, Tuple, List
from urllib.parse import urlparse

import aiofiles

from app.core.config import get_config
from app.core.proxy_pool import (
    build_http_proxies,
    get_current_proxy_from,
    rotate_proxy,
    should_rotate_proxy,
)
from app.core.exceptions import AppException, UpstreamException, ValidationException
from app.core.logger import logger
from app.core.storage import DATA_DIR
from app.services.reverse.assets_upload import AssetsUploadReverse
from app.services.reverse.utils.retry import retry_on_status
from app.services.reverse.utils.session import ResettableSession
from app.services.grok.utils.locks import _get_upload_semaphore, _file_lock


IMAGE_403_VARIANT_RETRIES = 2


class UploadService:
    """Assets upload service."""

    def __init__(self):
        self._session: Optional[ResettableSession] = None
        self._chunk_size = 64 * 1024

    async def create(self) -> ResettableSession:
        """Create or reuse a session."""
        if self._session is None:
            browser = get_config("proxy.browser")
            if browser:
                self._session = ResettableSession(impersonate=browser)
            else:
                self._session = ResettableSession()
        return self._session

    async def close(self):
        """Close the session."""
        if self._session:
            await self._session.close()
            self._session = None

    @staticmethod
    def _is_url(value: str) -> bool:
        """Check if the value is a URL."""
        try:
            parsed = urlparse(value)
            return bool(
                parsed.scheme and parsed.netloc and parsed.scheme in ["http", "https"]
            )
        except Exception:
            return False

    @staticmethod
    def _infer_mime(filename: str, fallback: str = "application/octet-stream") -> str:
        mime, _ = mimetypes.guess_type(filename)
        return mime or fallback

    @staticmethod
    async def _encode_b64_stream(chunks: AsyncIterator[bytes]) -> str:
        parts = []
        remain = b""
        async for chunk in chunks:
            if not chunk:
                continue
            chunk = remain + chunk
            keep = len(chunk) % 3
            if keep:
                remain = chunk[-keep:]
                chunk = chunk[:-keep]
            else:
                remain = b""
            if chunk:
                parts.append(base64.b64encode(chunk).decode())
        if remain:
            parts.append(base64.b64encode(remain).decode())
        return "".join(parts)

    async def _read_local_file(self, local_type: str, name: str) -> Tuple[str, str, str]:
        base_dir = DATA_DIR / "tmp"
        if local_type == "video":
            local_dir = base_dir / "video"
            mime = "video/mp4"
        else:
            local_dir = base_dir / "image"
            suffix = Path(name).suffix.lower()
            if suffix == ".png":
                mime = "image/png"
            elif suffix == ".webp":
                mime = "image/webp"
            elif suffix == ".gif":
                mime = "image/gif"
            else:
                mime = "image/jpeg"

        local_path = local_dir / name
        lock_name = f"ul_local_{hashlib.sha1(str(local_path).encode()).hexdigest()[:16]}"
        lock_timeout = max(1, int(get_config("asset.upload_timeout")))
        async with _file_lock(lock_name, timeout=lock_timeout):
            if not local_path.exists():
                raise ValidationException(f"Local file not found: {local_path}")
            if not local_path.is_file():
                raise ValidationException(f"Invalid local file: {local_path}")

            async def _iter_file() -> AsyncIterator[bytes]:
                async with aiofiles.open(local_path, "rb") as f:
                    while True:
                        chunk = await f.read(self._chunk_size)
                        if not chunk:
                            break
                        yield chunk

            b64 = await self._encode_b64_stream(_iter_file())
        filename = name or "file"
        return filename, b64, mime

    async def parse_b64(self, url: str) -> Tuple[str, str, str]:
        """Fetch URL content and return (filename, base64, mime)."""
        try:
            app_url = get_config("app.app_url") or ""
            if app_url and self._is_url(url):
                parsed = urlparse(url)
                app_parsed = urlparse(app_url)
                if (
                    parsed.scheme == app_parsed.scheme
                    and parsed.netloc == app_parsed.netloc
                    and parsed.path.startswith("/v1/files/")
                ):
                    parts = parsed.path.strip("/").split("/", 3)
                    if len(parts) >= 4:
                        local_type = parts[2]
                        name = parts[3].replace("/", "-")
                        return await self._read_local_file(local_type, name)

            lock_name = f"ul_url_{hashlib.sha1(url.encode()).hexdigest()[:16]}"
            timeout = float(get_config("asset.upload_timeout"))

            lock_timeout = max(1, int(get_config("asset.upload_timeout")))
            async with _file_lock(lock_name, timeout=lock_timeout):
                session = await self.create()
                active_proxy_key = None

                async def _do_fetch():
                    nonlocal active_proxy_key
                    active_proxy_key, proxy_url = get_current_proxy_from(
                        "proxy.base_proxy_url"
                    )
                    proxies = build_http_proxies(proxy_url)
                    response = await session.get(
                        url,
                        timeout=timeout,
                        proxies=proxies,
                        stream=True,
                    )
                    if response.status_code >= 400:
                        raise UpstreamException(
                            message=f"Failed to fetch: {response.status_code}",
                            details={"url": url, "status": response.status_code},
                        )
                    return response

                async def _on_retry(attempt: int, status_code: int, error: Exception, delay: float):
                    if active_proxy_key and should_rotate_proxy(status_code):
                        rotate_proxy(active_proxy_key)

                response = await retry_on_status(_do_fetch, on_retry=_on_retry)

                filename = url.split("/")[-1].split("?")[0] or "download"
                content_type = response.headers.get(
                    "content-type", ""
                ).split(";")[0].strip()
                if not content_type:
                    content_type = self._infer_mime(filename)
                if hasattr(response, "aiter_content"):
                    b64 = await self._encode_b64_stream(response.aiter_content())
                else:
                    b64 = base64.b64encode(response.content).decode()

                logger.debug(f"Fetched: {url}")
                return filename, b64, content_type
        except Exception as e:
            if isinstance(e, AppException):
                raise
            logger.error(f"Fetch failed: {url} - {e}")
            raise UpstreamException(f"Fetch failed: {str(e)}", details={"url": url})

    @staticmethod
    def format_b64(data_uri: str) -> Tuple[str, str, str]:
        """Format data URI to (filename, base64, mime)."""
        if not data_uri.startswith("data:"):
            raise ValidationException("Invalid file input: not a data URI")

        try:
            header, b64 = data_uri.split(",", 1)
        except ValueError:
            raise ValidationException("Invalid data URI format")

        if ";base64" not in header:
            raise ValidationException("Invalid data URI: missing base64 marker")

        mime = header[5:].split(";", 1)[0] or "application/octet-stream"
        b64 = re.sub(r"\s+", "", b64)
        if not mime or not b64:
            raise ValidationException("Invalid data URI: empty content")
        ext = mime.split("/")[-1] if "/" in mime else "bin"
        return f"file.{ext}", b64, mime

    @staticmethod
    def _decode_b64_payload(b64: str) -> bytes:
        try:
            return base64.b64decode(b64, validate=True)
        except binascii.Error as e:
            raise ValidationException(f"Invalid base64 payload: {e}")

    @staticmethod
    def _encode_b64_payload(raw: bytes) -> str:
        return base64.b64encode(raw).decode()

    @staticmethod
    def _png_with_text_chunk(raw: bytes, payload: bytes) -> Optional[bytes]:
        signature = b"\x89PNG\r\n\x1a\n"
        if not raw.startswith(signature):
            return None
        iend = raw.rfind(b"IEND")
        if iend < 8:
            return None
        chunk_start = iend - 4
        if chunk_start < len(signature):
            return None
        chunk_type = b"tEXt"
        chunk_data = b"grok2api\x00" + payload
        chunk_crc = zlib.crc32(chunk_type + chunk_data) & 0xFFFFFFFF
        chunk = (
            struct.pack(">I", len(chunk_data))
            + chunk_type
            + chunk_data
            + struct.pack(">I", chunk_crc)
        )
        return raw[:chunk_start] + chunk + raw[chunk_start:]

    @staticmethod
    def _jpeg_with_comment(raw: bytes, payload: bytes) -> Optional[bytes]:
        if len(raw) < 2 or raw[:2] != b"\xFF\xD8":
            return None
        segment_payload = payload[:65500]
        comment = b"\xFF\xFE" + struct.pack(">H", len(segment_payload) + 2) + segment_payload
        return raw[:2] + comment + raw[2:]

    @staticmethod
    def _gif_with_comment(raw: bytes, payload: bytes) -> Optional[bytes]:
        if not (raw.startswith(b"GIF87a") or raw.startswith(b"GIF89a")):
            return None
        trailer_index = raw.rfind(b"\x3B")
        if trailer_index == -1:
            return None
        blocks = []
        remaining = payload
        while remaining:
            chunk = remaining[:255]
            remaining = remaining[255:]
            blocks.append(bytes([len(chunk)]) + chunk)
        comment_ext = b"\x21\xFE" + b"".join(blocks) + b"\x00"
        return raw[:trailer_index] + comment_ext + raw[trailer_index:]

    @staticmethod
    def _webp_with_extra_chunk(raw: bytes, payload: bytes) -> Optional[bytes]:
        if len(raw) < 12 or raw[:4] != b"RIFF" or raw[8:12] != b"WEBP":
            return None
        chunk_type = b"G2AP"
        chunk_data = payload
        chunk = chunk_type + struct.pack("<I", len(chunk_data)) + chunk_data
        if len(chunk_data) % 2 == 1:
            chunk += b"\x00"
        mutated = raw + chunk
        riff_size = len(mutated) - 8
        return mutated[:4] + struct.pack("<I", riff_size) + mutated[8:]

    @classmethod
    def _build_image_variant(
        cls, filename: str, b64: str, mime: str, variant_index: int
    ) -> Optional[Tuple[str, str, str]]:
        if not isinstance(mime, str) or not mime.startswith("image/"):
            return None

        raw = cls._decode_b64_payload(b64)
        nonce = f"grok2api-{variant_index}-{secrets.token_hex(8)}".encode("utf-8")

        mutated = None
        if mime == "image/png":
            mutated = cls._png_with_text_chunk(raw, nonce)
        elif mime in ("image/jpeg", "image/jpg"):
            mutated = cls._jpeg_with_comment(raw, nonce)
        elif mime == "image/gif":
            mutated = cls._gif_with_comment(raw, nonce)
        elif mime == "image/webp":
            mutated = cls._webp_with_extra_chunk(raw, nonce)
        else:
            return None

        if not mutated or mutated == raw:
            return None

        logger.warning(
            f"Upload image variant prepared: filename={filename}, mime={mime}, variant={variant_index}"
        )
        return filename, cls._encode_b64_payload(mutated), mime

    async def check_format(self, file_input: str) -> Tuple[str, str, str]:
        """Check file input format and return (filename, base64, mime)."""
        if not isinstance(file_input, str) or not file_input.strip():
            raise ValidationException("Invalid file input: empty content")

        if self._is_url(file_input):
            return await self.parse_b64(file_input)

        if file_input.startswith("data:"):
            return self.format_b64(file_input)

        raise ValidationException("Invalid file input: must be URL or base64")

    async def _upload_single_file(self, file_input: str, token: str) -> Tuple[str, str]:
        """
        Upload file to Grok.

        Args:
            file_input: str, the file input.
            token: str, the SSO token.

        Returns:
            Tuple[str, str]: The file ID and URI.
        """
        async with _get_upload_semaphore():
            filename, b64, mime = await self.check_format(file_input)
            session = await self.create()

            current = (filename, b64, mime)
            for variant_index in range(IMAGE_403_VARIANT_RETRIES + 1):
                cur_filename, cur_b64, cur_mime = current
                logger.debug(
                    f"Upload prepare: filename={cur_filename}, type={cur_mime}, size={len(cur_b64)}, variant={variant_index}"
                )

                if not cur_b64:
                    raise ValidationException("Invalid file input: empty content")

                try:
                    response = await AssetsUploadReverse.request(
                        session,
                        token,
                        cur_filename,
                        cur_mime,
                        cur_b64,
                    )
                    result = response.json()
                    file_id = result.get("fileMetadataId", "")
                    file_uri = result.get("fileUri", "")
                    logger.info(
                        f"Upload success: {cur_filename} -> {file_id} (variant={variant_index})"
                    )
                    return file_id, file_uri
                except UpstreamException as e:
                    status = e.details.get("status") if isinstance(e.details, dict) else None
                    if status != 403 or variant_index >= IMAGE_403_VARIANT_RETRIES:
                        raise
                    next_variant = self._build_image_variant(
                        cur_filename, cur_b64, cur_mime, variant_index + 1
                    )
                    if not next_variant:
                        raise
                    logger.warning(
                        f"Upload failed with 403, retrying with mutated image variant={variant_index + 1}, filename={cur_filename}"
                    )
                    current = next_variant

    async def upload_files(
        self, file_inputs: List[str], token: str
    ) -> Tuple[List[Tuple[str, str]], str]:
        """
        Upload a batch of files using the same token.

        Returns:
            ([(file_id, file_uri), ...], used_token)
        """
        if not file_inputs:
            return [], token

        results: List[Tuple[str, str]] = []
        for file_input in file_inputs:
            results.append(await self._upload_single_file(file_input, token))
        return results, token

    async def upload_file(self, file_input: str, token: str) -> Tuple[str, str]:
        results, _ = await self.upload_files([file_input], token)
        return results[0]


__all__ = ["UploadService"]
