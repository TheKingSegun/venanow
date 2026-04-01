"""
pipeline/classifier.py

Transaction classification engine.
Stage 1: Rule-based (keyword + regex matching) — runs always.
Stage 2: ML-based (TF-IDF + Logistic Regression) — runs when trained model exists.

Categories (matching schema.sql):
  food, transport, rent, utilities, subscriptions,
  transfers, business, miscellaneous, income, freelance, investment
"""

from __future__ import annotations

import re
import pickle
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

from utils.logger import logger


# ── Category Rules ────────────────────────────────────────────────────────────
# Each rule: (category_slug, list_of_regex_patterns)
# Rules are evaluated in order — first match wins.

CLASSIFICATION_RULES: list[tuple[str, list[str]]] = [

    # ── Income / Credits (check first) ────────────────────────────────────────
    ("income", [
        r"\bsalary\b", r"\bpayroll\b", r"\bpay(ment)?\s+from\b", r"\bwage\b",
        r"\bmonthly\s+pay\b", r"\bincome\b", r"\bremuneration\b",
        r"\bbonus\b", r"\ballowance\b",
    ]),
    ("freelance", [
        r"\bfreelance\b", r"\bconsultancy\b", r"\bconsulting\s+fee\b",
        r"\bproject\s+pay(ment)?\b", r"\bupwork\b", r"\bfiverr\b",
        r"\bcontract\s+fee\b",
    ]),
    ("investment", [
        r"\bdividend\b", r"\binterest\s+credit\b", r"\bcoupon\s+payment\b",
        r"\bcowrywise\b", r"\brisevest\b", r"\bstanbic\s+investment\b",
        r"\bpiggyvest\b", r"\binvestment\s+return\b",
    ]),

    # ── Subscriptions (check before misc) ─────────────────────────────────────
    ("subscriptions", [
        r"\bnetflix\b", r"\bspotify\b", r"\bapple\s+(music|tv|icloud|one)\b",
        r"\bgoogle\s+(one|play|workspace|storage)\b", r"\bamazon\s+(prime|aws)\b",
        r"\bdstv\b", r"\bgotvmax\b", r"\bstarplus\b", r"\bshowmax\b",
        r"\bchatgpt\b", r"\bopenai\b", r"\bmidjourney\b", r"\bcanva\b",
        r"\badobe\b", r"\bmicrosoft\s+(365|office)\b", r"\bzoom\b",
        r"\bslack\b", r"\bnotion\b", r"\blinkedin\s+premium\b",
        r"\bantivirus\b", r"\bvpn\b", r"\bweb\s+hosting\b",
        r"\bdomain\s+renewal\b", r"\bsubscription\b",
        r"\bmonthly\s+(plan|package|charge)\b",
    ]),

    # ── Rent ──────────────────────────────────────────────────────────────────
    ("rent", [
        r"\brent\b", r"\bhouserent\b", r"\bhouse\s+rent\b",
        r"\bapartment\b", r"\bservice\s+charge\b", r"\bfacilities\b",
        r"\bproperty\s+(payment|charge)\b", r"\blandlord\b",
    ]),

    # ── Utilities ─────────────────────────────────────────────────────────────
    ("utilities", [
        r"\bekedc\b", r"\bikedc\b", r"\babeokuta\s+electricity\b",
        r"\bphcn\b", r"\belectric(ity)?\b", r"\bprepaid\s+token\b",
        r"\bpower\s+(bill|token)\b", r"\bnepa\b",
        r"\blawma\b", r"\bwaste\b", r"\bgarbage\b",
        r"\bwater\s+(bill|board)\b", r"\blwsc\b",
        r"\binternet\b", r"\bspectranet\b", r"\bsmile\b",
        r"\bairtel\s+home\b", r"\bmtn\s+home\b",
        r"\bgas\s+(bill|supply)\b", r"\bairgaz\b",
        r"\bservice\s+bill\b", r"\butility\b",
    ]),

    # ── Food & Dining ─────────────────────────────────────────────────────────
    ("food", [
        # Supermarkets
        r"\bshoprite\b", r"\bspar\b", r"\bnovare\b", r"\bcoldstone\b",
        r"\bpark\s+'n\s+shop\b", r"\bleventis\b", r"\bmarket\b",
        # Fast food
        r"\bdominos\b", r"\bpizza\s+hut\b", r"\bkfc\b", r"\bsuburban\b",
        r"\bchicken\s+republic\b", r"\beat\s+'n\s+go\b", r"\belectim\b",
        r"\bmr\s+biggs\b", r"\btantalizers\b", r"\bfunky\s+fresh\b",
        r"\bmama\s+cass\b", r"\bburger\s+king\b", r"\bhard\s+rock\s+cafe\b",
        # Delivery platforms
        r"\bjumia\s+food\b", r"\bglovo\b(?!\s+payment)", r"\bchowdeck\b",
        r"\bfoodcourt\b", r"\boluwole\b",
        # Generic
        r"\brestaurant\b", r"\bcafe\b", r"\bfood\b(?!\s+bank)",
        r"\beating\s+out\b", r"\bdining\b", r"\blunch\b", r"\bbreakfast\b",
        r"\bsnack\b", r"\bbakery\b", r"\bcanteen\b",
        # Grocery + drinks
        r"\bgrocery\b", r"\bdrink\b", r"\bbeer\b",
    ]),

    # ── Transport ─────────────────────────────────────────────────────────────
    ("transport", [
        r"\buber\b", r"\bbolt\b(?!\s+bank)", r"\bindriver\b", r"\bgokada\b",
        r"\bcab\b", r"\btaxi\b", r"\bride\s+(sharing|hailing)\b",
        r"\bdanfo\b", r"\bbrt\b", r"\bbike\b", r"\bokada\b",
        r"\bfuel\b", r"\bpetrol\b", r"\bdiesel\b", r"\bgas\s+station\b",
        r"\btotal\s+(oil|filling)\b", r"\bconoil\b", r"\boando\s+filling\b",
        r"\bflight\b", r"\bairline\b", r"\barik\b", r"\bair\s+peace\b",
        r"\bover(seas|flight)\b", r"\bairport\b",
        r"\bbus\s+(ticket|fare)\b", r"\btrain\b", r"\bferr(y|ies)\b",
        r"\bparking\b", r"\btoll\b",
    ]),

    # ── Business ──────────────────────────────────────────────────────────────
    ("business", [
        r"\binvoice\b", r"\bpurchase\s+order\b",
        r"\bwholesale\b", r"\bsupplier\b", r"\bvendor\b",
        r"\bcac\b", r"\btax\b(?!\s+refund)", r"\bvat\b", r"\bfirs\b",
        r"\bbusiness\s+(expense|payment)\b", r"\boffice\s+(supplies|rent)\b",
        r"\bpayroll\s+(run|payment)\b",
        r"\bshipping\b", r"\blogistics\b", r"\bwaybill\b",
        r"\bjumia\s+(seller|merchant)\b", r"\bpaystack\s+(settlement|payout)\b",
        r"\bflutterwave\s+settlement\b",
    ]),

    # ── Transfers (catch-all for P2P, interbank) ──────────────────────────────
    ("transfers", [
        r"\btransfer\s+(to|from)\b", r"\btrf\s+(to|from)\b",
        r"\bnip\s+transfer\b", r"\bneft\b", r"\brtgs\b",
        r"\bsend\s+money\b", r"\brecharge\b", r"\bairtime\b",
        r"\bopay\b", r"\bpalmpay\b", r"\bkuda\b", r"\bmoniepoint\b",
        r"\bchipper\b", r"\bcash\s+transfer\b",
    ]),
]

# Compile all patterns once
_COMPILED_RULES: list[tuple[str, list[re.Pattern]]] = [
    (cat, [re.compile(p, re.IGNORECASE) for p in patterns])
    for cat, patterns in CLASSIFICATION_RULES
]


# ── Rule-based Classifier ─────────────────────────────────────────────────────

def classify_transaction(description: str, tx_type: str) -> tuple[str, float]:
    """
    Classify a single transaction.

    Args:
        description: Cleaned transaction description.
        tx_type: 'debit' or 'credit'.

    Returns:
        (category_slug, confidence) where confidence is 0.0–1.0.
        Rule matches return 0.95; ML matches return model probability.
        Falls back to 'income' for credits, 'miscellaneous' for debits.
    """
    # Credits: run income rules first, then fall back to income
    if tx_type == "credit":
        for cat, patterns in _COMPILED_RULES:
            if cat in ("income", "freelance", "investment"):
                if any(p.search(description) for p in patterns):
                    return cat, 0.95
        return "income", 0.70  # Unmatched credit → income

    # Debits: run all debit categories
    for cat, patterns in _COMPILED_RULES:
        if cat in ("income", "freelance", "investment"):
            continue
        if any(p.search(description) for p in patterns):
            return cat, 0.95

    return "miscellaneous", 0.50  # Low confidence fallback


def classify_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply classification to the full DataFrame.
    Adds 'category' and 'confidence' columns.
    Tries ML classifier first (if trained), falls back to rules.
    """
    logger.info("Classifying transactions...")

    model = _load_ml_model()

    if model:
        logger.info("Using ML classifier.")
        df = _apply_ml_classifier(df, model)
    else:
        logger.info("Using rule-based classifier (ML model not trained yet).")
        df[["category", "confidence"]] = df.apply(
            lambda r: pd.Series(classify_transaction(r["description"], r["tx_type"])),
            axis=1
        )
        df["classified_by"] = "rule"

    n_misc = (df["category"] == "miscellaneous").sum()
    logger.info(
        f"Classification done. {len(df)} transactions classified. "
        f"{n_misc} ({n_misc/len(df)*100:.1f}%) fell through to miscellaneous."
    )
    return df


# ── ML Classifier (extensible) ────────────────────────────────────────────────

MODEL_PATH = Path("models/tx_classifier.pkl")


def _load_ml_model() -> Optional[object]:
    """Load the trained sklearn pipeline if it exists."""
    if MODEL_PATH.exists():
        try:
            with open(MODEL_PATH, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            logger.warning(f"Could not load ML model: {e}. Falling back to rules.")
    return None


def _apply_ml_classifier(df: pd.DataFrame, model) -> pd.DataFrame:
    """Apply trained ML model. Falls back to rules for low-confidence predictions."""
    probs = model.predict_proba(df["description"].tolist())
    labels = model.classes_
    df["confidence"] = probs.max(axis=1)
    df["category_ml"] = [labels[i] for i in probs.argmax(axis=1)]

    # Use rule-based for low-confidence predictions
    low_conf = df["confidence"] < 0.65
    rule_results = df[low_conf].apply(
        lambda r: pd.Series(classify_transaction(r["description"], r["tx_type"])),
        axis=1
    )
    df.loc[low_conf, ["category", "confidence"]] = rule_results.values
    df.loc[~low_conf, "category"] = df.loc[~low_conf, "category_ml"]
    df.loc[~low_conf, "classified_by"] = "ml"
    df.loc[low_conf, "classified_by"] = "rule"
    df = df.drop(columns=["category_ml"], errors="ignore")

    return df


def train_ml_classifier(
    descriptions: list[str],
    labels: list[str],
    save_path: Path = MODEL_PATH,
) -> dict:
    """
    Train a TF-IDF + Logistic Regression classifier on labeled data.
    This is called once you have enough manually labeled transactions.

    Args:
        descriptions : List of transaction descriptions.
        labels       : Corresponding category slugs.
        save_path    : Where to save the trained model.

    Returns:
        Training report dict with accuracy and class distribution.
    """
    from sklearn.pipeline import Pipeline
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import cross_val_score
    from collections import Counter

    logger.info(f"Training ML classifier on {len(descriptions)} samples...")

    pipeline = Pipeline([
        ("tfidf", TfidfVectorizer(
            ngram_range=(1, 2),
            min_df=2,
            max_features=10_000,
            analyzer="word",
            token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z]+\b",
        )),
        ("clf", LogisticRegression(
            max_iter=1000,
            class_weight="balanced",
            C=1.0,
            solver="lbfgs",
            multi_class="multinomial",
        )),
    ])

    cv_scores = cross_val_score(pipeline, descriptions, labels, cv=5, scoring="f1_macro")
    pipeline.fit(descriptions, labels)

    save_path.parent.mkdir(parents=True, exist_ok=True)
    with open(save_path, "wb") as f:
        pickle.dump(pipeline, f)

    report = {
        "samples": len(descriptions),
        "classes": dict(Counter(labels)),
        "cv_f1_macro_mean": round(cv_scores.mean(), 4),
        "cv_f1_macro_std":  round(cv_scores.std(), 4),
        "model_path": str(save_path),
    }
    logger.info(f"ML classifier trained. CV F1: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
    return report
