# Medikart Analytics — Deployment Guide
# Project: medikart-494016
# GitHub:  medikartpharmaatpune-blip/medikart-analytics

## STEP 1 — Install gcloud CLI on your Windows server
## ─────────────────────────────────────────────────
# Download from: https://cloud.google.com/sdk/docs/install
# Run installer, then in Command Prompt:

gcloud init
# → Sign in with your Google account
# → Select project: medikart-494016

gcloud auth application-default login
# → This lets Python scripts authenticate automatically


## STEP 2 — Enable GCP APIs
## ────────────────────────
gcloud services enable run.googleapis.com --project medikart-494016
gcloud services enable storage.googleapis.com --project medikart-494016
gcloud services enable cloudbuild.googleapis.com --project medikart-494016
gcloud services enable artifactregistry.googleapis.com --project medikart-494016
gcloud services enable iam.googleapis.com --project medikart-494016


## STEP 3 — Create Cloud Storage bucket
## ─────────────────────────────────────
gcloud storage buckets create gs://medikart-494016-data \
  --project medikart-494016 \
  --location asia-south1 \
  --uniform-bucket-level-access


## STEP 4 — Create Artifact Registry for Docker images
## ────────────────────────────────────────────────────
gcloud artifacts repositories create medikart \
  --repository-format docker \
  --location asia-south1 \
  --project medikart-494016


## STEP 5 — Create service account for GitHub Actions
## ────────────────────────────────────────────────────
gcloud iam service-accounts create medikart-deploy \
  --display-name "Medikart Deploy" \
  --project medikart-494016

# Grant permissions
gcloud projects add-iam-policy-binding medikart-494016 \
  --member "serviceAccount:medikart-deploy@medikart-494016.iam.gserviceaccount.com" \
  --role "roles/run.admin"

gcloud projects add-iam-policy-binding medikart-494016 \
  --member "serviceAccount:medikart-deploy@medikart-494016.iam.gserviceaccount.com" \
  --role "roles/storage.admin"

gcloud projects add-iam-policy-binding medikart-494016 \
  --member "serviceAccount:medikart-deploy@medikart-494016.iam.gserviceaccount.com" \
  --role "roles/artifactregistry.writer"

gcloud projects add-iam-policy-binding medikart-494016 \
  --member "serviceAccount:medikart-deploy@medikart-494016.iam.gserviceaccount.com" \
  --role "roles/cloudbuild.builds.editor"

gcloud projects add-iam-policy-binding medikart-494016 \
  --member "serviceAccount:medikart-deploy@medikart-494016.iam.gserviceaccount.com" \
  --role "roles/iam.serviceAccountUser"


## STEP 6 — Set up Workload Identity Federation (GitHub → GCP)
## ─────────────────────────────────────────────────────────────
# This lets GitHub Actions authenticate to GCP without a key file

gcloud iam workload-identity-pools create github-pool \
  --location global \
  --project medikart-494016

gcloud iam workload-identity-pools providers create-oidc github-provider \
  --location global \
  --workload-identity-pool github-pool \
  --issuer-uri "https://token.actions.githubusercontent.com" \
  --attribute-mapping "google.subject=assertion.sub,attribute.repository=assertion.repository" \
  --project medikart-494016

# Get the pool resource name (you'll need this below)
gcloud iam workload-identity-pools describe github-pool \
  --location global \
  --project medikart-494016 \
  --format "value(name)"
# → Save this value, it looks like:
#   projects/123456789/locations/global/workloadIdentityPools/github-pool

# Allow GitHub repo to use the service account
# Replace PROJECT_NUMBER with the number from above output
gcloud iam service-accounts add-iam-policy-binding \
  medikart-deploy@medikart-494016.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "principalSet://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/attribute.repository/medikartpharmaatpune-blip/medikart-analytics" \
  --project medikart-494016


## STEP 7 — Add GitHub Secrets
## ─────────────────────────────
# Go to: https://github.com/medikartpharmaatpune-blip/medikart-analytics
# Settings → Secrets and variables → Actions → New repository secret
#
# Add these two secrets:
#
# WIF_PROVIDER:
#   projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/providers/github-provider
#
# WIF_SERVICE_ACCOUNT:
#   medikart-deploy@medikart-494016.iam.gserviceaccount.com


## STEP 8 — Push code to GitHub
## ─────────────────────────────
# On your Windows machine (or any machine with git):

git clone https://github.com/medikartpharmaatpune-blip/medikart-analytics.git
cd medikart-analytics

# Copy the files from this package:
# agent/medikart_agent.py   → medikart_agent.py (also copy to D:\MEDIKART\)
# app/                      → app/
# .github/                  → .github/

git add .
git commit -m "Initial deployment"
git push origin main

# GitHub Actions will auto-build and deploy to Cloud Run
# Watch progress at: https://github.com/medikartpharmaatpune-blip/medikart-analytics/actions


## STEP 9 — Set up Windows Scheduled Task
## ────────────────────────────────────────
# Install agent dependencies:
py -3.12 -m pip install dbfread pandas google-cloud-storage

# Test it runs:
py -3.12 C:\Users\Administrator\MEDIKART\medikart_agent.py --folder "D:\CAREW" --once

# Create scheduled task (run every 15 minutes):
# Open Task Scheduler → Create Basic Task
# Name: Medikart Agent
# Trigger: Daily, repeat every 15 minutes
# Action: Start a program
#   Program: py
#   Arguments: -3.12 C:\Users\Administrator\MEDIKART\medikart_agent.py --folder "D:\CAREW" --once
#   Start in: C:\Users\Administrator\MEDIKART\

# OR via command line:
schtasks /create /tn "MedikartAgent" /tr "py -3.12 C:\Users\Administrator\MEDIKART\medikart_agent.py --folder D:\CAREW --once" /sc minute /mo 15 /ru SYSTEM /f


## STEP 10 — Access your dashboard
## ─────────────────────────────────
# After deployment (5-10 minutes after git push):
# Go to: https://console.cloud.google.com/run?project=medikart-494016
# Click on medikart-app → copy the URL
# Open it in any browser from anywhere!

# To add Google login protection (only your email can access):
gcloud run services update medikart-app \
  --no-allow-unauthenticated \
  --region asia-south1 \
  --project medikart-494016
# Then add your Google account as an invoker in IAM


## TROUBLESHOOTING
## ────────────────
# Check agent log:
#   C:\Users\Administrator\MEDIKART\medikart_agent.log
#
# Check what's in the bucket:
#   gcloud storage ls gs://medikart-494016-data/
#
# Check Cloud Run logs:
#   gcloud run logs read medikart-app --region asia-south1 --project medikart-494016
#
# Force redeploy:
#   git commit --allow-empty -m "force redeploy" && git push
