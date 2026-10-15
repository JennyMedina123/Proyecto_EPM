import shutil
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent

PASOS = [
    ("Sensores envían lecturas nuevas (OLTP)", ["scripts/generar_oltp.py"]),
    ("Extracción -> Bronze", ["scripts/extract.py"]),
    ("Verificación de Bronze", ["scripts/verificar_bronze.py"]),
    ("Limpieza -> Silver", ["scripts/transform_silver.py"]),
    ("Modelo dimensional -> Gold", ["scripts/transform_gold.py"]),
    ("Análisis y Benchmark", ["scripts/analyze.py"]),
]


def reset():
    sys.path.insert(0, str(RAIZ / "scripts"))
    import config as cfg
    if cfg.LAKEHOUSE.exists():
        shutil.rmtree(cfg.LAKEHOUSE)
        print("  Borrado: lakehouse/")
    if cfg.dialecto_oltp() == "sqlite":
        ruta = Path(cfg.OLTP_URL.replace("sqlite:///", ""))
        if ruta.exists():
            ruta.unlink()
            print("  Borrado: oltp/epm_oltp.sqlite")


def main():
    print("=" * 70)
    print("  MINI-LAKEHOUSE EPM — Telemetría IoT de centrales hidroeléctricas")
    print("=" * 70)
    if "--reset" in sys.argv:
        reset()

    t_total = time.time()
    for nombre, comando in PASOS:
        if not (RAIZ / comando[0]).exists():
            print(f"\n>>> {nombre}: pendiente ({comando[0]} aún no existe)")
            continue
        print(f"\n>>> {nombre}")
        t0 = time.time()
        resultado = subprocess.run([sys.executable] + comando, cwd=RAIZ)
        if resultado.returncode != 0:
            print(f"\nERROR en {comando[0]}. Pipeline detenido.")
            sys.exit(1)
        print(f"    Completado en {time.time() - t0:.1f}s")

    print(f"\nPipeline completo en {time.time() - t_total:.1f}s")


if __name__ == "__main__":
    main()