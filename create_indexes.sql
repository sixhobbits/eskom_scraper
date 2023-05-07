-- Add new columns in both tables to store the formatted date values
ALTER TABLE "station_build_up" ADD COLUMN "Formatted_Date" DATE;
ALTER TABLE "demand_side_actual_forecast_demand" ADD COLUMN "Formatted_Date" DATE;

-- Update the new columns with the formatted date values
UPDATE "station_build_up" SET "Formatted_Date" = strftime(substr("Date_Time_Hour_Beginning", 1, 10));
UPDATE "demand_side_actual_forecast_demand" SET "Formatted_Date" = strftime(substr("DateTimeKey", 1, 10));

CREATE INDEX idx_station_build_up_formatted_date ON "station_build_up"("Formatted_Date");
CREATE INDEX idx_demand_side_actual_forecast_demand_formatted_date ON "demand_side_actual_forecast_demand"("Formatted_Date");

