#include<vector>

#include "sql/operator/aggregate_physical_operator.h"
#include "storage/trx/trx.h"

RC AggregatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC AggregatePhysicalOperator::next()
{
  // already aggregated
  if (result_tuple_.cell_num()>0){
    return RC::RECORD_EOF;
  }

  RC rc = RC::SUCCESS;
  PhysicalOperator *oper = children_[0].get();

  std::vector<Value> result_cells;
  std::vector<Value> count_cells;
  std::vector<int>avg_place;
  int count_all=0;
  bool count_all_flag=false;
  bool firstmin=true;
  bool firstmax=true;
  Value temp;
  for(int cell_idx =0; cell_idx < (int)aggregations_.size(); cell_idx++){
    result_cells.push_back(temp);
    count_cells.push_back(temp);
    if(aggregations_[cell_idx]==AGGR_AVG){
      avg_place.push_back(cell_idx);
    }
  }
  while (RC::SUCCESS == (rc = oper->next())){
    // get tuple
    Tuple *tuple = oper->current_tuple();

    // do aggregate
    for (int cell_idx =0; cell_idx < (int)aggregations_.size(); cell_idx++){
      const AggrOp aggregation = aggregations_[cell_idx];
      
      Value cell;
      AttrType attr_type = AttrType::INTS;
      switch(aggregation){
        case AggrOp::AGGR_SUM:
          rc = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          result_cells[cell_idx].set_float(result_cells[cell_idx].get_float()+cell.get_float());
          break;
        case AggrOp::AGGR_MAX:
          rc = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          if(attr_type==AttrType::INTS||attr_type==AttrType::FLOATS)
          {
            if(firstmax)
            {
              result_cells[cell_idx].set_float(cell.get_float());
            }
            if(result_cells[cell_idx].get_float()<cell.get_float()){
             result_cells[cell_idx].set_float(cell.get_float());
            }
          }
          else if(attr_type==AttrType::CHARS)
          {
            if(firstmax)
            {
              result_cells[cell_idx].set_string(cell.get_string().c_str());
            }
            if(result_cells[cell_idx].get_string()<cell.get_string()){
             result_cells[cell_idx].set_string(cell.get_string().c_str());
            }
          }
          else if(attr_type==AttrType::DATES)
          {
            if(firstmax)
            {
              result_cells[cell_idx].set_date(cell.get_date());
            }
            if(result_cells[cell_idx].get_date()<cell.get_date()){
             result_cells[cell_idx].set_date(cell.get_date());
            }
          }
          break;
        case AggrOp::AGGR_MIN:
          rc = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          if(attr_type==AttrType::INTS||attr_type==AttrType::FLOATS)
          {
            if(firstmin)
            {
              result_cells[cell_idx].set_float(cell.get_float());
            }
            if(result_cells[cell_idx].get_float()>cell.get_float()){
              result_cells[cell_idx].set_float(cell.get_float());
            }
          }
          else if(attr_type==AttrType::CHARS)
          {
            if(firstmin)
            {
              result_cells[cell_idx].set_string(cell.get_string().c_str());
            }
            if(result_cells[cell_idx].get_string()>cell.get_string()){
             result_cells[cell_idx].set_string(cell.get_string().c_str());
            }
          }
          else if(attr_type==AttrType::DATES)
          {
            if(firstmax)
            {
              result_cells[cell_idx].set_date(cell.get_date());
            }
            if(result_cells[cell_idx].get_date()>cell.get_date()){
             result_cells[cell_idx].set_date(cell.get_date());
            }
          }
          break;
        case AggrOp::AGGR_COUNT:
          result_cells[cell_idx].set_int(result_cells[cell_idx].get_int()+1);
          break;
        case AggrOp::AGGR_COUNT_ALL:
          result_cells[cell_idx].set_int(result_cells[cell_idx].get_int()+1);
          // count_all++;
          // count_all_flag=true;
          break;
        case AggrOp::AGGR_AVG:
          rc = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          result_cells[cell_idx].set_float(result_cells[cell_idx].get_float()+cell.get_float());  
          count_cells[cell_idx].set_int(count_cells[cell_idx].get_int()+1);
          break;
        
        default:
          return RC::UNIMPLENMENT;
      }
    }
    firstmin=false;
    firstmax=false;
  }
  if(rc == RC::RECORD_EOF){
    rc = RC::SUCCESS;
  }

  while(!avg_place.empty()){
    int a =avg_place.back();
    result_cells[a].set_float(result_cells[a].get_float()/count_cells[a].get_int());
    avg_place.pop_back();
  }

  // if(count_all_flag){
  //   while(!result_cells.empty())
  //   {
  //     result_cells.pop_back();
  //   }
  //   result_cells.push_back(temp);
  //   result_cells[0].set_int(count_all);
  // }

  result_tuple_.set_cells(result_cells);
  
  return rc;
}

RC AggregatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

Tuple* AggregatePhysicalOperator::current_tuple()
{
  return &result_tuple_;
}

void AggregatePhysicalOperator::add_aggregation(const AggrOp aggregation)
{
  aggregations_.push_back(aggregation);
}
