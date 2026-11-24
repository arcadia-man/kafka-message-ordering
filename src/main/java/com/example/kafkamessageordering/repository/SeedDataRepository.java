package com.example.kafkamessageordering.repository;

import com.example.kafkamessageordering.entity.SeedData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SeedDataRepository extends JpaRepository<SeedData, String> {
}
