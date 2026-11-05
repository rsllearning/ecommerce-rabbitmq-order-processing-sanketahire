#!/usr/bin/env bash
# Displays the backend consumer worker processing logs
LOG_FILE="/Users/sanket.ahire/.gemini/antigravity-cli/brain/1f27af8d-bf7c-426f-a98a-8ebac35ffeb7/.system_generated/tasks/task-176.log"

echo "=========================================================================="
echo "  RabbitMQ Consumer Workers: Live Event Processing Logs"
echo "=========================================================================="
echo ""
tail -n 35 "$LOG_FILE"
echo ""
echo "=========================================================================="
