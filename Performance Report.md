




Act Days
	(days touched)
	calculation getPerformanceDaysTouched(wo, taskfleetIdbytaskid, fleetcategorybyfleetid)
	filiter validFleetIds
	
**user_count** 
**total_hours** 
**task_days_touched** = **total_Hours** / **user_count** * 0.105
	if **task_days_touched** > 1 
		than 1
	else return **task_days_touched**
Sum up to services level. 


getValidFleetIdsForTaskService()
	The fleet assigned to the task are within the service  ^1242a7

Time entires with no out date are excluded 



# Calculations 

- task -> daysTouched
	```php
		    public function daysTouched(): Attribute
	    {
	        return Attribute::get(function () {
	            $users_count       = $this->user_count;
	            $total_labor_hours = $this->labor_hours;
	
	            if ($users_count <= 0) {
	                return 0.0;
	            }
	
	            $adj_value = ($total_labor_hours / $users_count) * 0.105;
	
	            return min($adj_value, 1);
	```
	^task-days-touched
	
	



	

# Columns 

Act Days 
	Calculated [[#^task-days-touched]]
	filters:
		historical-tasks
		
