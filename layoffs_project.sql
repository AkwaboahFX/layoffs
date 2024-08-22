select *
from layoffs;

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,
row_number() over(partition by
company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(partition by
company, location, industry, 
total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'Ada';

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;

--- satanderdizing data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%' ;

select `date`
from layoffs_staging2;
 
 update layoffs_staging2
 set `date` = str_to_date(`date`, '%m/%d/%Y');
 
 alter table layoffs_staging2
 modify column `date` date;
 
 select *
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null;
 
delete
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null;

 select *
 from layoffs_staging2;
 
 alter table layoffs_staging2
 drop column row_num;


 select max(total_laid_off), max(percentage_laid_off)
 from layoffs_staging2;
 
 select *
 from layoffs_staging2
 where percentage_laid_off = 1
 order by funds_raised_millions desc;
 
 select company, sum(total_laid_off)
 from layoffs_staging2
 group by company
 order by 2 desc;
 
 select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

 select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by `date`
order by 2 desc;

 select substring(`date`, 1, 7) as month, sum(total_laid_off) as laid_off
from layoffs_staging2
where substring(`date`, 1, 7)
group by month
order by 1 asc;

with rolling_total as
(
select substring(`date`, 1, 7) as `month`, sum(total_laid_off) as laid_off
from layoffs_staging2
where substring(`date`, 1, 7)
group by month
order by 1 asc 
) 
select `month`, laid_off, 
sum(laid_off) over(order by `month`) as rolling_stone
from rolling_total;

select company, year(`date`) as year, sum(total_laid_off) as laid_off
from layoffs_staging2
group by company,  `date`
order by 3 desc;

with laid_off_year(company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by company,  year(`date`)
),
company_year as
(
select *, dense_rank() over(
partition by years order by total_laid_off desc) as fired
from laid_off_year
where years is not null
order by fired asc
)
select *
from company_year
where fired <= 5
order by total_laid_off asc;


 
 
 
 
 





































CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


















