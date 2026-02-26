import os
import pandas as pd

def run_inference(model, X_test, customer_ids, run_path):

    preds = model.predict_proba(X_test)[:, 1]

    predictions_df = pd.DataFrame({
        "customer_id": customer_ids,
        "propensity_score": preds
    })

    predictions_path = os.path.join(run_path, "predictions")
    os.makedirs(predictions_path, exist_ok=True)

    predictions_df.to_csv(
        os.path.join(predictions_path, "predictions.csv"),
        index=False
    )

    print("Predictions saved.")

    return predictions_df