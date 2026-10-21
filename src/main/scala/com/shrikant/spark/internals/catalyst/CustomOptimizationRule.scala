package com.shrikant.spark.internals.catalyst

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.Filter

class CustomOptimizationRule extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case f @ Filter(condition, child) =>
        println(s"\n[CATALYST CUSTOM RULE] Intercepted Filter condition: $condition")

        // Dynamically check if the condition involves status = "LEGACY_CODE"
        condition match {
          case EqualTo(attr: AttributeReference, lit: Literal) if attr.name == "status" && lit.value == "LEGACY_CODE" =>
            println(s"[CATALYST CUSTOM RULE] >>> MATCH FOUND! Rewriting status 'LEGACY_CODE' to 'ARCHIVED'...")
            Filter(EqualTo(attr, Literal("ARCHIVED")), child)
          case _ =>
            f
        }
    }
  }
}