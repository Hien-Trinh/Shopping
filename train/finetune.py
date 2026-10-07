"""Fine-tune bge-small end-to-end as the student (docs/specs/step-6l.3.md).

Plain Python, so it runs where there is a GPU. The project needs Python 3.14, which Colab
doesn't ship, so install through uv there (it brings its own Python and the locked versions):

    !curl -LsSf https://astral.sh/uv/install.sh | sh
    !git clone -b <branch> https://github.com/Hien-Trinh/Shopping.git && cd Shopping && \
        ~/.local/bin/uv sync --group finetune                      # about 2 min
    %env HF_TOKEN=<a write token>                                   # or Colab's secrets
    !cd Shopping && ~/.local/bin/uv run python train/finetune.py --smoke --out /content/smoke
    !cd Shopping && ~/.local/bin/uv run python train/finetune.py --out /content/student-ft --push

On the Mac, `uv run --group finetune python train/finetune.py --smoke`. Not imported by `src/`.

Writes to `--out`: `onnx/model.onnx` and `onnx/model_quantized.onnx` (the encoder; fastembed does
the CLS pooling and normalization), the tokenizer files, `head.npz` (the linear head in
`catalog.student.Softmax`'s format) and `train.json` (what produced them). `--push` uploads the
folder to the Hub repo the eval downloads (`python -m catalog.student --download`).
"""

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import torch
from torch.nn import functional as F
from transformers import AutoModel, AutoTokenizer

from catalog import entry, student

BASE = "BAAI/bge-small-en-v1.5"  # the fp32 checkpoint of the model the workers run as int8 ONNX
MAX_LENGTH = 128  # title plus 200 description characters is about 64 tokens
SEED = 0
CHECKPOINT = "checkpoint.pt"


class Encoder(torch.nn.Module):
    """The transformer with a tensor output, for ONNX export and the CLS vectors."""

    def __init__(self, base: str):
        super().__init__()
        self.model = AutoModel.from_pretrained(base)

    def forward(self, input_ids, attention_mask, token_type_ids):
        return self.model(
            input_ids=input_ids, attention_mask=attention_mask, token_type_ids=token_type_ids
        ).last_hidden_state


class Student(torch.nn.Module):
    """CLS pooling, unit normalization (as bge does) and a linear head over the Categories."""

    def __init__(self, encoder: Encoder, n_classes: int):
        super().__init__()
        self.encoder = encoder
        self.head = torch.nn.Linear(encoder.model.config.hidden_size, n_classes)

    def vectors(self, batch):
        return F.normalize(self.encoder(**batch)[:, 0], dim=-1)

    def forward(self, batch):
        return self.head(self.vectors(batch))


def split(smoke: bool):
    """6l.2's split: eval titles dropped, 10% of each source held out, seed 0."""
    labels = [p for lp, _ in student.EVALS.values() for p in lp if p.exists()]
    titles = [x["title"] for x in student._labels(labels)]
    sources = [student.without_titles(student.load_training([p]), titles) for p in student.TRAIN]
    fit, held = student.held_out(sources, seed=SEED)
    if smoke:
        fit, held = fit[:200], held[:50]
    return fit, held


def batches(tokenizer, rows, classes, size, device, shuffle, seed=SEED):
    index = {c: i for i, c in enumerate(classes)}
    order = np.arange(len(rows))
    if shuffle:
        np.random.default_rng(seed).shuffle(order)
    for start in range(0, len(rows), size):
        chunk = [rows[i] for i in order[start : start + size]]
        enc = tokenizer(
            [student._text(r["title"], r["description"]) for r in chunk],
            padding=True,
            truncation=True,
            max_length=MAX_LENGTH,
            return_tensors="pt",
        )
        if "token_type_ids" not in enc:
            enc["token_type_ids"] = torch.zeros_like(enc["input_ids"])
        y = torch.tensor([index.get(r["category"], -1) for r in chunk])
        yield {k: v.to(device) for k, v in enc.items()}, y.to(device)


@torch.no_grad()
def exact(model, tokenizer, rows, classes, size, device) -> float:
    model.eval()
    hits = 0
    for batch, y in batches(tokenizer, rows, classes, size, device, shuffle=False):
        hits += int((model(batch).argmax(dim=-1) == y).sum())
    return hits / len(rows)


def signature(a, fit, classes) -> dict:
    """What a checkpoint must match to be resumed: the data and the hyperparameters that shape
    the weights and the optimizer (not the epoch count, so a run can be continued for more)."""
    return {
        "data_sha256": hashlib.sha256(
            json.dumps([[r["title"], r["description"], r["category"]] for r in fit]).encode()
        ).hexdigest(),
        "classes": len(classes),
        "batch": a.batch,
        "lr": a.lr,
        "head_lr": a.head_lr,
        "smoke": a.smoke,
    }


def train(a, fit, held, device) -> tuple[Student, AutoTokenizer, list[str], dict]:
    torch.manual_seed(SEED)
    classes = sorted({r["category"] for r in fit})
    sig = signature(a, fit, classes)
    tokenizer = AutoTokenizer.from_pretrained(BASE)
    model = Student(Encoder(BASE), len(classes)).to(device)
    optim = torch.optim.AdamW(
        [
            {"params": model.encoder.parameters(), "lr": a.lr},
            {"params": model.head.parameters(), "lr": a.head_lr},
        ],
        weight_decay=0.01,
    )
    steps_per_epoch = -(-len(fit) // a.batch)
    total = steps_per_epoch * a.epochs
    warm = max(1, total // 10)
    sched = torch.optim.lr_scheduler.LambdaLR(
        optim, lambda s: s / warm if s < warm else max(0.0, (total - s) / max(1, total - warm))
    )
    log = {"held_out_exact": [], "best_epoch": None}
    best, start_epoch = None, 0
    checkpoint = a.out / CHECKPOINT
    if checkpoint.exists():  # a Colab disconnect or a spot eviction: pick up at the next epoch
        saved = torch.load(checkpoint, map_location=device, weights_only=False)
        if saved.get("signature") != sig:
            print(
                "ignoring a checkpoint from another run (data or hyperparameters differ)",
                file=sys.stderr,
            )
            saved = None
    else:
        saved = None
    if saved:
        model.load_state_dict(saved["model"])
        optim.load_state_dict(saved["optim"])
        sched.load_state_dict(saved["sched"])
        log, best, start_epoch = saved["log"], saved["best"], saved["epoch"] + 1
        print(f"resuming after epoch {saved['epoch'] + 1}", file=sys.stderr)
    use_amp = device.type == "cuda"
    scaler = torch.amp.GradScaler(enabled=use_amp)
    for epoch in range(start_epoch, a.epochs):
        model.train()
        t0 = time.time()
        for step, (batch, y) in enumerate(
            batches(tokenizer, fit, classes, a.batch, device, shuffle=True, seed=SEED + epoch)
        ):
            with torch.autocast(device.type, dtype=torch.float16, enabled=use_amp):
                loss = F.cross_entropy(model(batch), y)
            if not torch.isfinite(loss):
                raise RuntimeError(f"training diverged at epoch {epoch + 1} step {step + 1}")
            optim.zero_grad(set_to_none=True)
            scaler.scale(loss).backward()
            scaler.unscale_(optim)
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            scaler.step(optim)
            scaler.update()
            sched.step()
            if a.smoke and step + 1 >= a.smoke_steps:
                break
        acc = exact(model, tokenizer, held, classes, a.batch, device)
        log["held_out_exact"].append(round(acc, 4))
        print(
            f"epoch {epoch + 1}/{a.epochs}: held-out exact {acc:.1%}, {time.time() - t0:.0f} s",
            file=sys.stderr,
        )
        if best is None or acc > best[0]:
            best = (acc, {k: v.detach().cpu().clone() for k, v in model.state_dict().items()})
            log["best_epoch"] = epoch + 1
        torch.save(
            {
                "model": model.state_dict(),
                "optim": optim.state_dict(),
                "sched": sched.state_dict(),
                "log": log,
                "best": best,
                "epoch": epoch,
                "signature": sig,
            },
            checkpoint.with_suffix(".tmp"),
        )
        os.replace(checkpoint.with_suffix(".tmp"), checkpoint)  # whole or absent, never truncated
    model.load_state_dict(best[1])
    return model, tokenizer, classes, log


def export(model: Student, tokenizer, classes, held, out: Path, device) -> dict:
    """The encoder to ONNX (fp32 and int8), the head to head.npz; the fp32 ONNX must agree with
    torch on 100 held-out texts or nothing is kept."""
    import onnxruntime as ort
    from onnxruntime.quantization import QuantType, quantize_dynamic

    model.eval().to("cpu")
    (out / "onnx").mkdir(parents=True, exist_ok=True)
    tokenizer.model_max_length = MAX_LENGTH  # fastembed reads it: serve at the training length
    tokenizer.save_pretrained(out)
    model.encoder.model.config.save_pretrained(out)
    sample, _ = next(batches(tokenizer, held[:4], classes, 4, torch.device("cpu"), shuffle=False))
    names = ["input_ids", "attention_mask", "token_type_ids"]
    torch.onnx.export(
        model.encoder,
        tuple(sample[n] for n in names),
        str(out / "onnx" / "model.onnx"),
        input_names=names,
        output_names=["last_hidden_state"],
        dynamic_axes={n: {0: "batch", 1: "sequence"} for n in [*names, "last_hidden_state"]},
        opset_version=17,
        dynamo=False,
    )
    check, _ = next(
        batches(tokenizer, held[:100], classes, 100, torch.device("cpu"), shuffle=False)
    )
    with torch.no_grad():
        want = model.vectors(check).numpy()
    session = ort.InferenceSession(str(out / "onnx" / "model.onnx"))
    hidden = session.run(None, {n: check[n].numpy() for n in names})[0]
    got = student.unit(hidden[:, 0])
    gap = float(np.abs(got - want).max())
    if gap > 1e-3:
        raise RuntimeError(f"ONNX disagrees with torch: max |diff| {gap:.2e} on 100 texts")
    quantize_dynamic(
        str(out / "onnx" / "model.onnx"),
        str(out / "onnx" / "model_quantized.onnx"),
        weight_type=QuantType.QInt8,
    )
    w = model.head.weight.detach().numpy().T.astype(np.float64)
    b = model.head.bias.detach().numpy().astype(np.float64)
    head = student.Softmax(classes, w, b)
    head.save(out / student.HEAD)
    return {"onnx_vs_torch_max_abs_diff": gap, **_int8_cost(out, tokenizer, held, classes, head)}


def _int8_cost(out: Path, tokenizer, held, classes, head) -> dict:
    """Held-out exact through each ONNX file and the head, the path the eval serves: what int8
    costs against fp32 (step-6l.3.md)."""
    import onnxruntime as ort

    names = ["input_ids", "attention_mask", "token_type_ids"]
    labels = [r["category"] for r in held]
    exact = {}
    for key, name in (("fp32", "model.onnx"), ("int8", "model_quantized.onnx")):
        session = ort.InferenceSession(str(out / "onnx" / name))
        got = []
        for batch, _ in batches(tokenizer, held, classes, 100, torch.device("cpu"), shuffle=False):
            x = student.unit(session.run(None, {n: batch[n].numpy() for n in names})[0][:, 0])
            got += [p[0] for p in head.predict(x)]
        exact[f"held_out_exact_{key}"] = round(
            sum(g == y for g, y in zip(got, labels, strict=True)) / len(labels), 4
        )
    return exact


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--out", type=Path, default=Path("models/student-ft"))
    p.add_argument("--epochs", type=int, default=None, help="4, or 1 with --smoke")
    p.add_argument("--batch", type=int, default=64)
    p.add_argument("--lr", type=float, default=5e-5, help="encoder learning rate")
    p.add_argument("--head-lr", type=float, default=1e-3)
    p.add_argument("--push", action="store_true", help=f"upload --out to {student.FT_REPO}")
    p.add_argument("--repo", default=student.FT_REPO)
    p.add_argument("--smoke", action="store_true", help="200 rows, 1 epoch, a few steps, no push")
    p.add_argument("--smoke-steps", type=int, default=3)
    a = p.parse_args(argv)
    a.epochs = a.epochs or (1 if a.smoke else 4)
    if a.smoke:
        a.push = False
    if a.push:  # fail before the GPU run, not after it
        from huggingface_hub import HfApi

        HfApi().whoami()
    device = torch.device(
        "cuda"
        if torch.cuda.is_available()
        else "mps"
        if torch.backends.mps.is_available()
        else "cpu"
    )
    a.out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    fit, held = split(a.smoke)
    model, tokenizer, classes, log = train(a, fit, held, device)
    check = export(model, tokenizer, classes, held, a.out, device)
    (a.out / CHECKPOINT).unlink(missing_ok=True)
    info = {
        "base": BASE,
        "rows_fit": len(fit),
        "rows_held_out": len(held),
        "classes": len(classes),
        "epochs": a.epochs,
        "batch": a.batch,
        "lr": a.lr,
        "head_lr": a.head_lr,
        "max_length": MAX_LENGTH,
        "seed": SEED,
        "smoke": a.smoke,
        "weight_decay": 0.01,
        "grad_clip": 1.0,
        "mixed_precision": device.type == "cuda",
        **signature(a, fit, classes),
        "git_sha": _git_sha(),
        "device": str(device)
        + (f" {torch.cuda.get_device_name(0)}" if device.type == "cuda" else ""),
        "platform": platform.platform(),
        "versions": {m.__name__: m.__version__ for m in (torch, np)}
        | {
            "transformers": __import__("transformers").__version__,
            "onnxruntime": __import__("onnxruntime").__version__,
        },
        "wall_seconds": round(time.time() - t0),
        **log,
        **check,
    }
    (a.out / "train.json").write_text(json.dumps(info, indent=2) + "\n")
    print(json.dumps(info, indent=2))
    if a.push:
        from huggingface_hub import HfApi

        api = HfApi()
        api.create_repo(a.repo, repo_type="model", exist_ok=True)
        api.upload_folder(folder_path=str(a.out), repo_id=a.repo, repo_type="model")
        print(f"pushed to https://huggingface.co/{a.repo}", file=sys.stderr)


def _git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


if __name__ == "__main__":
    entry.exit_with(main)
